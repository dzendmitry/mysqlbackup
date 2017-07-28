package main

import (
	"github.com/dzendmitry/logger"
	"os"
	"path"
	"archive/tar"
	"io"
	"compress/gzip"
)

const (
	WriterCmdChLen = 10000
	ArchivePath = "archive"
)

type writerCmd struct {
	server string
	table  string
	data   []string
	answerCh chan workerAnswer
}

type fileWriter struct {
	cmdCh chan writerCmd
	workers int
	files map[string]*os.File
	log logger.ILogger
}

var w *fileWriter

func NewWriter() *fileWriter {
	if w != nil {
		return w
	}
	return &fileWriter{
		cmdCh: make(chan writerCmd, WriterCmdChLen),
		files: make(map[string]*os.File),
		log: logger.InitConsoleLogger("FILEWRITER"),
	}
}

func (w *fileWriter) Run() {
	for cmd := range w.cmdCh {
		if len(cmd.data) == 0 {
			w.workers -= 1
			if w.workers == 0 {
				for len(w.cmdCh) > 0 {
					w.writeData(<-w.cmdCh)
				}
				break
			}
		} else {
			w.writeData(cmd)
		}
	}
}

func (w *fileWriter) Stop() {
	for fPath, file := range w.files {
		if _, err := file.Seek(0, 0); err != nil {
			w.log.Warnf("Error seek file %v: %+v", file.Name(), err)
			file.Close()
			continue
		}
		stat, err := file.Stat()
		if err != nil {
			w.log.Warnf("Error stat file %v: %+v", file.Name(), err)
			file.Close()
			continue
		}
		if stat.Size() == 0 {
			w.log.Warnf("Error file size is zero %v", file.Name())
			file.Close()
			os.Remove(fPath)
			continue
		}
		fTarPath := fPath + ".tar.gz"
		fTar, err := os.Create(fTarPath)
		if err != nil {
			w.log.Warnf("Error creating tar file %v: %+v", fTarPath, err)
			file.Close()
			continue
		}
		gzw := gzip.NewWriter(fTar)
		tw := tar.NewWriter(gzw)
		hdr := &tar.Header{
			Name: stat.Name(),
			Mode: 0600,
			Size: int64(stat.Size()),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			w.log.Warnf("Error can't writer header to tar %v: %+v", fTar.Name(), err)
			file.Close()
			os.Remove(fTarPath)
			continue
		}
		if _, err := io.Copy(tw, file); err != nil {
			w.log.Warnf("Error archiving file %v: %+v", file.Name(), err)
			file.Close()
			os.Remove(fTarPath)
			continue
		}
		if err := tw.Close(); err != nil {
			w.log.Warnf("Error closing tar archive %v: %+v", fTar.Name(), err)
			file.Close()
			os.Remove(fTarPath)
			continue
		}
		if err := gzw.Close(); err != nil {
			w.log.Warnf("Error closing tar archive %v: %+v", fTar.Name(), err)
			file.Close()
			os.Remove(fTarPath)
			continue
		}
		file.Close()
		os.Remove(fPath)
	}
	w.log.Infof("Everything is done")
}

func (w *fileWriter) writeData(cmd writerCmd) bool {
	serverDir := path.Join(ArchivePath, cmd.server)
	filePath := path.Join(serverDir, cmd.table + ".csv")
	if err := os.MkdirAll(serverDir, os.ModeDir | os.ModePerm); err != nil {
		cmd.answerCh <- workerAnswer{err}
		return false
	}
	f, ok := w.files[filePath]
	if !ok {
		var err error
		f, err = os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0600)
		if err != nil {
			cmd.answerCh <- workerAnswer{err}
			return false
		}
		w.files[filePath] = f
	}
	for _, csvString := range cmd.data {
		if _, err := f.WriteString(csvString); err != nil {
			cmd.answerCh <- workerAnswer{err}
			return false
		}
	}
	cmd.answerCh <- workerAnswer{nil}
	return true
}

func (w *fileWriter) setWorkers(workers int) {
	w.workers = workers
}