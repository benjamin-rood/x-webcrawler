package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/net/html"
	"golang.org/x/sync/errgroup"
)

/**
NOTE: This code is not Windows-compatible. Sorry.

Code Overview:

The crawl begins with the initially provided search url argument. The HTML document is copied from the given url, and that spawns a worker that scans it for links.

The HTML doc for every visited http(s) URL is written to a file in the provided output directory argument,
with the filename an escaped version of the URL string e.g. `https:%2F%2Fgithub.com%2Fgolang-standards%2Fproject-layout%2Ftree%2Fmaster%2Fcmd`
which can be safely written and read to disk on UNIX-like systems.


If I had more time, in order of priority:
1. Add the test coverage
2. The html node traversal should be parallelised
3. Use the the child urls to save html docs to a tree structure to disk and using that would make resuming a lot faster because you could resume from the 'leaves' of the file tree
4. Scale up and down the amount of crawlers depending on current length of the CrawlQueue?
*/

// I'm using globals just for convenience + time constraints
// (it's the easiest approach for the 'toy' program like this)
var (
	// we will convert the given `searchUrl` into a URL type to make everything easier
	RootURL   *url.URL
	OutputDir string
	// these could easily be initialised in main, but I am in a rush
	// As each HTML page is parsed for links, valid child links get added to buffered channel "queue"
	// this also works to cap the amount of spawned `childLinkExtractor` instances
	CrawlQueue = make(chan string, 50)
	// unbuffered channel as blocking queue to enforce single writes to disk
	WriteFile = make(chan htmlFile)
	// laziest concurrent-safe solution to keeping track of what has been visited,
	Discovered sync.Map
	// very simple error values to handle expected cases
	HTTPNotFound       = errors.New("page not found")
	HTTPContentNotHTML = errors.New("page content not HTML")
	URLNotChildLink    = errors.New("url can't be resolved as a child link")
	// errors are just values
	Finished = errors.New("finished")
)

func parseFlagsAndSetup() error {
	var err error
	// make sure we are not running on Windows!
	if runtime.GOOS == "windows" {
		return fmt.Errorf("crawler is not Windows-compatible")
	}

	// Define the command-line flags
	searchUrlArg := flag.String("searchUrl", "", "the URL to search")
	outputDirArg := flag.String("outputDir", "", "the output directory")

	// Parse the command-line arguments
	flag.Parse()
	// Check if the searchUrl flag is set
	if *searchUrlArg == "" {
		return fmt.Errorf("usage: crawler -searchUrl x -outputDir y")
	}
	// Check if the outputDir flag is set
	if *outputDirArg == "" {
		return fmt.Errorf("usage: crawler -searchUrl x -outputDir y")
	}

	// proceed with setup
	// we will convert the given `searchUrl` into a URL type to make everything easier down the line
	RootURL, err = url.Parse(*searchUrlArg)
	if err != nil {
		return fmt.Errorf("searchUrl arg '%s' cannot be parsed into a proper URL: %s", *searchUrlArg, err)
	}
	if RootURL.Scheme != "http" && RootURL.Scheme != "https" {
		return fmt.Errorf("searchUrl arg '%s' is not an http or https URL", *searchUrlArg)
	}
	// check if the outputDir already exists to see if we need to resume prior searching
	absDirPath, err := filepath.Abs(*outputDirArg)
	if err != nil {
		return fmt.Errorf("outputDir arg '%s' could not be resolved", *outputDirArg)
	}
	dirEntries, readDirErr := os.ReadDir(absDirPath)
	if readDirErr == nil {
		// just silently resume
		for _, dirEntry := range dirEntries {
			if !dirEntry.IsDir() {
				// since all filenames in the directory (should be!) an espcaed url
				// we must unescape it before adding it to the Discovered map
				visitedUrl, err := url.PathUnescape(dirEntry.Name())
				if err != nil {
					// skip instead of panicking?
					continue
				}
				Discovered.Store(visitedUrl, true)
			}
		}
	} else if os.IsNotExist(readDirErr) {
		// create the directory
		if err := os.MkdirAll(absDirPath, 0755); err != nil {
			return fmt.Errorf("could not create specified output directory '%s'", absDirPath)
		}
	} else {
		// ack! we've got an serious error, abort abort
		return readDirErr
	}
	OutputDir = absDirPath
	// setup completed!
	return nil
}

func main() {
	// initialise, set up HTML fetchers and HTML link-finders
	// can be closed with cancel signal or timeout
	// set up our async servers, error handling, and graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eg, egCtx := errgroup.WithContext(ctx)

	// start a go function to stop all goroutines in the error group when
	// the error group context is done
	go func() {
		// wait on the context to complete
		<-egCtx.Done()
		// cancel any others running
		cancel()
	}()

	// setup signal handling to cancel everything if we receive a signal (e.g. Ctrl-C)
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
		// wait on a signal
		sig := <-sigChan
		// log the signal
		log.Print("exiting on signal...", sig)
		// cancel all running
		cancel()
		os.Exit(1)
	}()

	// start writer inside error group
	w := func() error { return writer(egCtx) }
	eg.Go(w)

	var inactivityCount atomic.Uint32
	inactivityCount.Store(0)
	// spawn 5 crawlers in error group
	spawnCrawlers(eg, egCtx, &inactivityCount)

	if err := parseFlagsAndSetup(); err != nil {
		// ABORT ON ANY ERRORS
		cancel()
		log.Fatal(err)
	}

	// simplistic completion detection, in lieu of some better algorithmic way
	// flawed because this approach doesn't detect if there is a deadlock somewhere
	inactivity := time.NewTicker(50 * time.Millisecond)
	eg.Go(func() error {
		for {
			select {
			// check for no waiting work every 100ms
			case <-inactivity.C:
				// if both channels are empty for 1s or more, exit
				if len(CrawlQueue) == 0 && len(WriteFile) == 0 {
					inactivityCount.Add(1)
					if inactivityCount.Load() >= 10 {
						// assume crawling complete
						log.Println("finishing")
						return Finished
					}
				}
			case <-egCtx.Done():
				return egCtx.Err()
			}
		}
	})
	// begin timer
	start := time.Now()
	// seed the crawling with the starting url
	if RootURL == nil {
		log.Fatal("RootURL nil")
	}
	CrawlQueue <- RootURL.String()

	// wait for the errorgroup to finish
	if err := eg.Wait(); err != nil {
		// finished natually, or due to a fault?
		if !errors.Is(err, Finished) {
			// this might not be all the errors, but will be the first received
			log.Fatal(err)
		}
		log.Println(err.Error())
		cancel()
		log.Printf("time elapsed = %v seconds\n", time.Now().Sub(start).Seconds())
		return
	}
}

func spawnCrawlers(group *errgroup.Group, groupCtx context.Context, counter *atomic.Uint32) {
	// Spawn 5 worker goroutines in the given error group
	for i := 0; i < 32; i++ {
		// TODO: make use of workerID in the logging for each crawler
		// workerID := i
		group.Go(func() error {
			for {
				select {
				case url, ok := <-CrawlQueue:
					if !ok {
						return nil // CrawlQueue closed, exit
					}
					// reset no activity counter
					counter.Swap(0)
					h, err := fetchHTML(url)
					if err != nil {
						// log it, no point in crashing the program
						log.Println("couldn't fetch html doc at", url)
						continue // skip doing anything else with this
					}
					// extract child links, will add links to the CrawlQueue
					go childLinkExtractor(url, h)
					WriteFile <- htmlFile{url, h} // blocks until file has been picked up to be written

				case <-groupCtx.Done():
					return groupCtx.Err()
				}
			}
		})
	}
}

type htmlFile struct {
	Url     string
	HtmlDoc string
}

func writer(ctx context.Context) error {
	for {
		select {
		case hf, ok := <-WriteFile:
			if !ok {
				return nil // WriteFile closed, exit
			}
			// TODO: add checks to make sure `hf` is in a valid state, e.g. if empty
			escapedUrl := url.PathEscape(hf.Url) // use the url (escaped) as the filename
			writePath := filepath.Join(OutputDir, escapedUrl)
			if err := os.WriteFile(writePath, []byte(hf.HtmlDoc), 0644); err != nil {
				return fmt.Errorf("Failed to write file %s: %v", writePath, err)
			}
		case <-ctx.Done():
			return nil
		}
	}
}

// fetcher to get HTML doc from a given url
func fetchHTML(url string) (string, error) {
	log.Printf("fetching HTML from url <%s>", url)
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return "", HTTPNotFound
	}
	contentType := resp.Header.Get("Content-Type")
	if !strings.HasPrefix(contentType, "text/html") {
		return "", HTTPContentNotHTML
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

// given the contents of an html document, will parse and extract all valid urls inside <a> tags in a page
// where a valid url is one that resolves to being a "child" of the parent URL
func childLinkExtractor(parentLink, parentHTML string) {
	parentURL, err := url.Parse(parentLink)
	if err != nil {
		log.Fatal(err)
	}
	// parsing for links stolen from: https://pkg.go.dev/golang.org/x/net/html#example-Parse
	doc, err := html.Parse(strings.NewReader(parentHTML))
	if err != nil {
		// FIXME: don't panic, just report the error
		log.Fatal(err)
	}
	var traverseForLinks func(*html.Node)
	traverseForLinks = func(n *html.Node) {
		childLink := ""
		if n.Type == html.ElementNode && n.Data == "a" {
			for _, attr := range n.Attr {
				if attr.Key == "href" {
					link, err := parentURL.Parse(attr.Val)
					if err != nil {
						continue // something malformed, just skip
					}
					// only want HTTP links, that resolve to a child of the given parentURL, and that we haven't visited before
					if link.Scheme == "http" || link.Scheme == "https" {
						if !link.IsAbs() {
							link = RootURL.ResolveReference(link)
						}
						if strings.HasPrefix(basePath(link.String()), basePath(RootURL.String())) {
							// strip any in-page anchor from link url
							childLink = strings.Split(link.String(), "#")[0]
						}
						if childLink != "" {
							_, alreadyDiscovered := Discovered.LoadOrStore(childLink, false)
							if !alreadyDiscovered {
								// add to the queue of urls to visit
								// will block if queue is full
								CrawlQueue <- childLink
							}
						}
					}
					break
				}
			}
		}
		// traverse any child nodes if present
		if n.FirstChild != nil {
			for c := n.FirstChild; c != nil; c = c.NextSibling {
				traverseForLinks(c)
			}
		}
	}

	// begin html tree traversal for links
	traverseForLinks(doc)
}

func basePath(link string) string {
	pathParts := strings.Split(link, "/")
	return strings.TrimRight(link, pathParts[len(pathParts)-1])
}

type subtree struct {
	node  *html.Node
	depth int
}

func traverseForLinksParallel(ctx context.Context, root *html.Node) {
	// Create a buffered channel to store subtrees that need to be processed.
	subtreeQueue := make(chan subtree, 10)
	finish := make(chan struct{})

	// Create a wait group to wait for all workers to finish.
	var wg sync.WaitGroup

	// Add the root node to the work queue.
	subtreeQueue <- subtree{root, 0}

	// Start work
	wg.Add(1)
	func() {
		defer wg.Done()
		// primary worker
		go func() {
			for len(subtreeQueue) > 0 {
				select {
				case <-ctx.Done():
					return

				default:
					st, ok := <-subtreeQueue
					if ok {
						processSubtree(ctx, st, subtreeQueue)
					}
				}
			}
			// queue empty, signal other workers to stop
			close(finish)
			return
		}()
		// work stealers
		for {
			select {
			case <-ctx.Done():
				return

			case <-finish:
				return

			default:
				st, ok := stealWork(subtreeQueue)
				if ok {
					if len(CrawlQueue) < 20 {
						wg.Add(1)
						go func() {
							defer wg.Done()
							processSubtree(ctx, st, subtreeQueue)
						}()
					} else {
						// put the stolen work back on the work queue
						subtreeQueue <- st
					}
				}
			}
		}
	}()

	// Wait for all workers to finish.
	wg.Wait()
}

func processSubtree(ctx context.Context, st subtree, subtreeQueue chan<- subtree) {
	node := st.node
	depth := st.depth

	if node == nil {
		// The subtree has been processed. Return.
		return
	}

	// extractChildLink will add any child urls not previously discovered
	// to the CrawlQueue channel
	// parsing of the node for a suitable link can be done in parallel
	go extractChildLink(node)

	// breadth-first traversal of the HTML node tree
	// TODO: implement depth restriction
	for c := node.FirstChild; c != nil; c = c.NextSibling {
		select {
		case <-ctx.Done():
			// Context cancelled. Stop processing subtrees.
			return
		default:
			// Context not cancelled. Add subtree to the queue.
			subtreeQueue <- subtree{c, depth + 1}
		}
	}
}

func extractChildLink(n *html.Node) {
	childLink := ""
	if n.Type == html.ElementNode && n.Data == "a" {
		for _, attr := range n.Attr {
			if attr.Key == "href" {
				link, err := RootURL.Parse(attr.Val)
				if err != nil {
					continue // something malformed, just skip
				}
				// only want HTTP links, that resolve to a child of the given parentURL, and that we haven't visited before
				if link.Scheme == "http" || link.Scheme == "https" {
					if !link.IsAbs() {
						link = RootURL.ResolveReference(link)
					}
					if strings.HasPrefix(basePath(link.String()), basePath(RootURL.String())) {
						// strip any in-page anchor from link url
						childLink = strings.Split(link.String(), "#")[0]
					}
					if childLink != "" {
						_, alreadyDiscovered := Discovered.LoadOrStore(childLink, false)
						if !alreadyDiscovered {
							// add to the queue of urls to visit
							// will block if queue is full
							CrawlQueue <- childLink
						}
					}
				}
				break
			}
		}
	}
}

func stealWork(subtreeQueue <-chan subtree) (subtree, bool) {
	select {
	case st := <-subtreeQueue:
		return st, true
	default:
		// No work available to steal.
		return subtree{}, false
	}
}
