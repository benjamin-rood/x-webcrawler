.PHONY: build clean

all: build

build:
	go build -o crawler

clean:
	rm -f crawler