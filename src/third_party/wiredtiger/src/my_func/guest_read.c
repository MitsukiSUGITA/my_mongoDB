#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <string.h>

// ここを lspci で確認したアドレスに書き換えてください (例: 0000:00:04.0)
#define PCI_DEVICE_ADDR "0000:00:05.0"
#define SHM_SIZE (8 * 1024 * 1024)

int main() {
    char path[256];
    // BAR2 に直接アクセスするためのパス
    snprintf(path, sizeof(path), "/sys/bus/pci/devices/%s/resource2", PCI_DEVICE_ADDR);

    int fd = open(path, O_RDWR);
    if (fd < 0) {
        perror("open resource2");
        fprintf(stderr, "ヒント: PCIアドレスが正しいか、sudo で実行しているか確認してください。\n");
        return 1;
    }

    // PCIリソースを直接マッピング
    char *shared_mem = mmap(NULL, SHM_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    
    if (shared_mem == MAP_FAILED) {
        perror("mmap");
        close(fd);
        return 1;
    }

    printf("ゲスト: 共有メモリ(BAR2)から読み込みます...\n");
    printf("データ: %c %c %c\n", shared_mem[0], shared_mem[1], shared_mem[2]);

    munmap(shared_mem, SHM_SIZE);
    close(fd);
    return 0;
}