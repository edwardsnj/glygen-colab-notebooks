#!/bin/sh

for f in $@; do
  g=`basename "$f" .py`
  .venv/bin/lazydocs --output-path stdout --src-base-url "https://github.com/edwardsnj/glygen-colab-notebooks/blob/main" $f > $g.md
done
