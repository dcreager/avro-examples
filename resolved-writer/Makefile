all: test

CC ?= gcc

resolved-writer: resolved-writer.c
	@$(CC) -g -O2 -o $@ $< -lavro

build: resolved-writer

test: build
	@./resolved-writer

resolved-writer.html: resolved-writer.c
	@rocco $<

doc: resolved-writer.html

clean:
	@rm resolved-writer
	@rm resolved-writer.html
