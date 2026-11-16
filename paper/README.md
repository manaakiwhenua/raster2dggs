## Rebuilding the PDF

```bash
cd ~/projects/dggs-joss-paper
docker run --rm --volume "$PWD":/data --user $(id -u):$(id -g) \
    --env JOURNAL=joss openjournals/inara
```

`paper.pdf` and `jats/` are build outputs.
