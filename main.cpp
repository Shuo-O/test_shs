#include "demo.h"
#include <unistd.h>

int main(){
    DemoMd md;
    md.start();
    while(1)
        sleep(10);
    return 0;
}