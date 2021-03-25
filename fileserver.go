package main

import (
    "log"
    "net/http"
)

func main() {
    fs := http.FileServer(http.Dir("/tmp"))
    log.Fatal(http.ListenAndServe(":8000", fs))
}
