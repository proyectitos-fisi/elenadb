module fisi/elenadb/cli

go 1.22

toolchain go1.22.4

require (
	fisi/elenadb v0.0.0
	github.com/fatih/color v1.17.0
	github.com/go-json-experiment/json v0.0.0-20240524174822-2d9f40f7385b
	github.com/hokaccha/go-prettyjson v0.0.0-20211117102719-0474bc63780f
	github.com/proyectitos-fisi/elena-prompt v1.0.5
	github.com/urfave/cli/v2 v2.27.2
)

require (
	github.com/cpuguy83/go-md2man/v2 v2.0.4 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mattn/go-runewidth v0.0.3 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/xrash/smetrics v0.0.0-20240312152122-5f08fbb34913 // indirect
	golang.org/x/sys v0.18.0 // indirect
)

replace fisi/elenadb v0.0.0 => ../../
