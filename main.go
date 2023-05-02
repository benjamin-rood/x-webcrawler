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
	CrawlQueue = make(chan string, 20)
	// unbuffered channel as blocking queue to enforce single writes to disk
	WriteFile = make(chan htmlFile)
	// laziest concurrent-safe solution to keeping track of what has been visited,
	// can't be bothered implementing a regular map controlled by a mutex
	Discovered sync.Map
	// very simple error values to handle expected cases
	HTTPNotFound       = errors.New("page not found")
	HTTPContentNotHTML = errors.New("page content not HTML")
	URLNotChildLink    = errors.New("url can't be resolved as a child link")
)

func parseFlagsAndSetup() {
	// make sure we are not running on Windows!
	if runtime.GOOS == "windows" {
		log.Fatal("crawler is not Windows-compatible")
	}

	// Define the command-line flags
	searchUrlArg := flag.String("searchUrl", "", "the URL to search")
	outputDirArg := flag.String("outputDir", "", "the output directory")

	// Parse the command-line arguments
	flag.Parse()
	// Check if the searchUrl flag is set
	if *searchUrlArg == "" {
		log.Fatal("usage: crawler -searchUrl x -outputDir y")
	}
	// Check if the outputDir flag is set
	if *outputDirArg == "" {
		log.Fatal("usage: crawler -searchUrl x -outputDir y")
	}

	// proceed with setup
	// we will convert the given `searchUrl` into a URL type to make everything easier down the line
	RootURL, err := url.Parse(*searchUrlArg)
	if err != nil {
		log.Fatalf("searchUrl arg '%s' cannot be parsed into a proper URL: %s", *searchUrlArg, err)
	}
	if RootURL.Scheme != "http" && RootURL.Scheme != "https" {
		log.Fatalf("searchUrl arg '%s' is not an http or https URL", *searchUrlArg)
	}
	// check if the outputDir already exists to see if we need to resume prior searching
	absDirPath, err := filepath.Abs(*outputDirArg)
	if err != nil {
		log.Fatalf("outputDir arg '%s' could not be resolved", *outputDirArg)
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
			log.Fatalf("could not create specified output directory '%s'", absDirPath)
		}
	} else {
		// ack! we've got an serious error, abort abort
		log.Fatal(readDirErr)
	}
	OutputDir = absDirPath
	// setup completed!
}

func main() {
	// initialise, set up HTML fetchers and HTML link-finders
	// can be closed with cancel signal or timeout
	// set up our async servers, error handling, and gracefull shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eg, egCtx := errgroup.WithContext(ctx)

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
	}()

	parseFlagsAndSetup() // will call os.Exit(1) if setup fails

	spawnCrawlers(eg, egCtx)
	// seed the crawling with the starting url
	CrawlQueue <- RootURL.String()
	// wait for the errorgroup to finish
	if err := eg.Wait(); err != nil {
		// this might not be all the errors, but will be the first received
		log.Println(err)
	}
}

func spawnCrawlers(group *errgroup.Group, errCtx context.Context) {
	timeout := time.NewTicker(time.Second)
	// Spawn 5 worker goroutines in the given error group
	for i := 0; i < 5; i++ {
		// TODO: make use of workerID in the logging for each crawler
		// workerID := i
		group.Go(func() error {
			for {
				select {
				case url, ok := <-CrawlQueue:
					if !ok {
						return nil // CrawlQueue closed, exit
					}
					h, err := fetchHTML(url)
					if err != nil {
						// log it, no point in crashing the program
						log.Println("couldn't fetch html doc at", url)
						continue // skip doing anything else with this
					}
					// spawn a goroutine to extract child links, will add links to the CrawlQueue
					go childLinkExtractor(h)
					WriteFile <- htmlFile{h, url} // blocks until file has been picked up to be written

				// VERY VERY VERY LAZY & FLAWED APPROACH!
				case <-timeout.C:
					if len(CrawlQueue) == 0 && len(WriteFile) == 0 {
						return nil
					}

				case <-errCtx.Done():
					return errCtx.Err()
				}
			}
		})
	}
}

type htmlFile struct {
	HtmlDoc string
	Url     string
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
			writePath := filepath.Join(OutputDir, escapedUrl+".html")
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
func childLinkExtractor(parentHTML string) {
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
					link, err := RootURL.Parse(attr.Val)
					if err != nil {
						continue // something malformed, just skip
					}
					// only want HTTP links, that resolve to a child of the given parentURL, and that we haven't visited before
					if link.Scheme == "http" || link.Scheme == "https" {
						if link.IsAbs() {
							if link.Host == RootURL.Host && strings.HasPrefix(link.Path, RootURL.Path) {
								childLink = link.String()
							}
						} else {
							resolved := RootURL.ResolveReference(link)
							if resolved.Host == RootURL.Host && strings.HasPrefix(resolved.Path, RootURL.Path) {
								childLink = resolved.String()
							}
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

func appendUnique[T comparable](slice []T, elem T) []T {
	for _, x := range slice {
		if x == elem {
			return slice // Element already exists, return original slice
		}
	}
	// Element does not exist, return appended result
	return append(slice, elem)
}
