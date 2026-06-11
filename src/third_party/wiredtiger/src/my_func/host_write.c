#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

int main() {
    int fd = open("/dev/shm/mongo_bitmap", O_RDWR);
    if (fd < 0) { perror("open"); return 1; }

    // 8MBをマッピング
    char *shared_mem = mmap(NULL, 8 * 1024 * 1024, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (shared_mem == MAP_FAILED) { perror("mmap"); return 1; }

    printf("ホスト: 共有メモリの先頭に 'A' (0x41) を書き込みます...\n");
    shared_mem[0] = 'A';
    shared_mem[1] = 'B';
    shared_mem[2] = 'C';

    printf("書き込み完了。ゲスト側で確認してください。\n");

    munmap(shared_mem, 8 * 1024 * 1024);
    close(fd);
    return 0;
}