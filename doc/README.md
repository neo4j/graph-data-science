# GDS Docs README


## Setup

We use asciidoc for writing documentation, and we render it to both HTML and PDF.


### Rendering the docs locally

First, you have to run `npm install`.
Second, you have to run `npm install @neo4j-antora/antora-page-roles --save`.
After having done this once, you needn't do it again.

To build and view the docs locally, you can use `npm run start`.


### A note on inline LaTeX: you can't

Currently, our toolchain cannot render LaTeX snippets into PDF (works for HTML tho!). So we are unable to use it.

What you can do though is use _cursive_, `monospace` and `.svg` images for anything more complicated. https://www.codecogs.com/latex/eqneditor.php is helpful for inputting LaTeX and outputting `.svg` images, or any other image format for that matter. We seem to use `.svg` so maybe stick to that.


### Documentation authoring

We use a few conventions for documentation source management:

1. Write one sentence per line.
   This keeps the source readable.
   A newline in the source has no effect on the produced content.
2. Use two empty lines ahead of a new section.
   This keeps the source more readable.
