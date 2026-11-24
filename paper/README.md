## Rebuilding the PDF

```bash
cd paper
docker run --rm --volume "$PWD":/data --user $(id -u):$(id -g) \
    --env JOURNAL=joss openjournals/inara
```

`paper.pdf` and `jats/` are build outputs.
