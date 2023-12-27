package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"time"

	"github.com/djherbis/times"
)

func setupPurgeStaleFilesRoutine() *time.Ticker {
	ticker := time.NewTicker(time.Duration(24) * time.Hour) // purge files once a day
	go func() {
		for repoName := range config.Repos {
			purgeStaleFiles(config.CacheDir, config.PurgeFilesAfter, config.KeepFiles, repoName)
		}
		for {
			select {
			case <-ticker.C:
				for repoName := range config.Repos {
					purgeStaleFiles(config.CacheDir, config.PurgeFilesAfter, config.KeepFiles, repoName)
				}
			}
		}
	}()

	return ticker
}

type archPackage struct {
	name  string
	repo  string
	count int
	files []packageFile
}

type packageFile struct {
	path  string
	atime time.Time
	mtime time.Time
	size  int64
}

// purgeStaleFiles purges files in the pacoloco cache
// it recursively scans `cacheDir`/pkgs and if the file access time is older than
// `now` - purgeFilesAfter(seconds) then the file gets removed
// however, we always keep at least the number of packages per pkg defined in the variable `keep`
func purgeStaleFiles(cacheDir string, purgeFilesAfter int, keep int, repoName string) {
	// safety check, so we don't unintentionally wipe the whole cache
	if purgeFilesAfter == 0 {
		log.Fatalf("Stopping because purgeFilesAfter=%v and that would purge the whole cache", purgeFilesAfter)
	}

	// examples at https://regex101.com/r/aYVIcM/2
	re, err := regexp.Compile("^(?P<pkgname>[a-zA-Z0-9-_+]+)-(?P<pkgver>[0-9.:a-zA-Z+]+)-(?P<pkgrel>[0-9]+)-(?P<arch>[a-zA-Z0-9_]+).pkg.tar.(?:zst|xz)$")
	if err != nil {
		log.Fatalf("Stopping because of error %s", err)
	}

	removeIfOlder := time.Now().Add(time.Duration(-purgeFilesAfter) * time.Second)
	pkgDir := filepath.Join(cacheDir, "pkgs", repoName)
	var packageSize int64
	var packageNum int64
	var packages = make(map[string]archPackage)
	walkfn := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}

		t := times.Get(info)
		p := packageFile{
			path:  path,
			mtime: t.ModTime(),
			atime: t.AccessTime(),
			size:  info.Size(),
		}
		parsedFile := re.FindStringSubmatch(filepath.Base(path))
		// skip files that do not match regex like myrepo.db
		if len(parsedFile) == 0 {
			return nil
		}
		mapName := fmt.Sprintf("%s/%s", repoName, parsedFile[1])
		if val, ok := packages[mapName]; ok {
			val.count += 1
			val.files = append(val.files, p)
			packages[mapName] = val
		} else {
			packages[mapName] = archPackage{
				name:  parsedFile[1],
				repo:  repoName,
				count: 1,
				files: []packageFile{p},
			}
		}
		return nil
	}
	if err := filepath.Walk(pkgDir, walkfn); err != nil {
		log.Println(err)
	}

	for _, pkg := range packages {
		slices.SortFunc(pkg.files, func(a, b packageFile) int { return a.mtime.Compare(b.mtime) })

		for index, val := range pkg.files {
			packageSize += val.size
			packageNum++
			if index >= len(pkg.files)-keep {
				continue
			}
			if val.atime.Before(removeIfOlder) {
				log.Printf("Remove stale file %v as its access time (%v) is too old", val.path, val.atime)
				if err := os.Remove(val.path); err != nil {
					log.Print(err)
				}

				if err := os.Remove(val.path + ".sig"); err != nil {
					log.Print(err)
				}
				packageSize -= val.size
				packageNum--
			}
		}
	}

	cachePackageGauge.WithLabelValues(repoName).Set(float64(packageNum))
	cacheSizeGauge.WithLabelValues(repoName).Set(float64(packageSize))
}
