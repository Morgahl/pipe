package main

import (
	"bufio"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Morgahl/pipe"
)

const (
	COUNT   = 4
	CH_SIZE = COUNT * COUNT
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

func main() {
	dir, err := getDir(os.Args)
	if err != nil {
		log.Fatalln(err)
	}

	start := time.Now()
	results := pipeline(CH_SIZE, true, dir).
		MapErrorSinkAsync(COUNT, openFile, logError("error opening file")).
		MapErrorSinkAsync(COUNT, multiHash, logError("error multi hashing file")).
		MapErrorSinkAsync(COUNT, closeFile, logError("error closing file")).
		// Tap(logFileFound).
		Window(time.Second, newResults, compileResult).
		Tap(logAny[*Results]).
		Reduce(&Results{}, compileResults)

	log.Println(results)
	log.Printf("took=%s", time.Since(start))
}

func pipeline(size int, recurse bool, dir string) pipe.Tail[*FileInfo] {
	head, tail := pipe.New[*FileInfo](size)

	go func() {
		defer head.Close()
		if err := filepath.WalkDir(dir, walkFunc(dir, recurse, head)); err != nil {
			log.Printf("error walking directory: dir=%s, err=%s", dir, err)
		}
	}()

	return tail
}

func walkFunc(dir string, recurse bool, out chan<- *FileInfo) func(string, fs.DirEntry, error) error {
	return func(path string, d fs.DirEntry, err error) error {
		if d.IsDir() {
			// If we have a Directory that has errored: log error; SkipDir
			if err != nil {
				log.Printf("path=%s, err=%s\n", path, err)
				return fs.SkipDir
			}

			// If we have a Directory that is not the starting dir and recurse is disabled: SkipDir
			if !recurse && dir != path {
				return fs.SkipDir
			}

			// If we are a Directory that hasn't errored: don't emit; continue walking
			return nil
		}

		// If we are a normal file that has errored: don't emit; log error; continue walking
		if err != nil {
			log.Printf("path=%s, err=%s\n", path, err)
			return nil
		}

		// We have a file and we don't seem to have angered the powers that be: emit; continue walking
		out <- &FileInfo{
			Path:  path,
			Entry: d,
		}

		return nil
	}
}

func getDir(args []string) (string, error) {
	if len(args) < 2 {
		return "", errors.New("must pass directory path after binary name")
	}

	dir := args[1]
	if !fs.ValidPath(dir) {
		return "", fmt.Errorf("must pass valid path: %s", dir)
	}

	dir, err := filepath.Abs(dir)
	if err != nil {
		return "", fmt.Errorf("must pass valid path: %s", err)
	}

	dir = strings.Trim(dir, "\"")

	return dir, nil
}

func compileResult(fi *FileInfo, results *Results) *Results {
	results.Found++
	results.TotalDuration += fi.Duration
	return results
}

func compileResults(result, results *Results) *Results {
	results.Found += result.Found
	results.TotalDuration += result.TotalDuration
	return results
}

type FileInfo struct {
	Path   string
	Entry  fs.DirEntry
	File   *os.File
	Buffer io.Reader
	MD5    []byte
	SHA1   []byte
	SHA256 []byte
	SHA512 []byte
	Start    time.Time
	End      time.Time
	Duration time.Duration
}

type Results struct {
	Found         int
	TotalDuration time.Duration
}

func newResults() *Results {
	return new(Results)
}

func (r Results) Average() time.Duration {
	if r.Found == 0 {
		return 0
	}

	return r.TotalDuration / time.Duration(r.Found)
}

func (r Results) String() string {
	return fmt.Sprintf("Processed: %d, Avg: %s, Tot: %s", r.Found, r.Average(), r.TotalDuration)
}

var fileBuffers = sync.Pool{
	New: func() any {
		return bufio.NewReaderSize(nil, 128*1024)
	},
}

func openFile(fi *FileInfo) (*FileInfo, error) {
	fi.Start = time.Now()

	f, err := os.Open(fi.Path)
	if err != nil {
		return &FileInfo{}, err
	}

	fi.File = f

	bf := fileBuffers.Get().(*bufio.Reader)
	bf.Reset(f)
	fi.Buffer = bf

	return fi, nil
}

func closeFile(fi *FileInfo) (*FileInfo, error) {
	fileBuffers.Put(fi.Buffer)
	fi.Buffer = nil

	err := fi.File.Close()
	fi.File = nil
	fi.End = time.Now()
	fi.Duration = fi.End.Sub(fi.Start)

	return fi, err
}

var buffers = sync.Pool{
	New: func() any {
		buf := make([]byte, 32*1024)
		return &buf
	},
}

func multiHash(fi *FileInfo) (*FileInfo, error) {
	buf := buffers.Get().(*[]byte)
	defer buffers.Put(buf)

	md5 := md5.New()
	sha1 := sha1.New()
	sha256 := sha256.New()
	sha512 := sha512.New()
	w := io.MultiWriter(md5, sha1, sha256, sha512)
	if _, err := io.CopyBuffer(w, fi.Buffer, *buf); err != nil {
		return &FileInfo{}, err
	}

	fi.MD5 = md5.Sum(nil)
	fi.SHA1 = sha1.Sum(nil)
	fi.SHA256 = sha256.Sum(nil)
	fi.SHA512 = sha512.Sum(nil)

	return fi, nil
}

func logFileFound(fi *FileInfo) {
	info, _ := fi.Entry.Info()
	log.Printf(
		"Found file: name=%s, md5=%.8X, sha1=%.8X, sha256=%.8X, sha512=%.8X, size=%d, start=%s, end=%s, took=%s",
		fi.Entry.Name(), fi.MD5, fi.SHA1, fi.SHA256, fi.SHA512, info.Size(), fi.Start.Format(time.StampMicro), fi.End.Format(time.StampMicro), fi.Duration,
	)
}

func logError(message string) func(error) {
	return func(err error) {
		log.Println(message, err)
	}
}

func logAny[T any](t T) {
	log.Println(t)
}
