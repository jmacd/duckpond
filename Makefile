all: linux

linux:
	docker buildx build -t duckpond:latest --platform linux/amd64 . 

cp: linux
	@rm -f duckpond.tar duckpond.tar.zst
	@docker save -o duckpond.tar duckpond:latest
	@zstd duckpond.tar
	@scp duckpond.tar.zst jmacd@casparwater.us:
	echo Uploaded.
