#include "demo.h"
#include <iostream>
#include <unistd.h>

int main(){
    DemoMd md;
    if (!md.start()) {
        std::cerr << "failed to start DemoMd: " << md.last_error() << std::endl;
        return 1;
    }
    while(1)
        sleep(10);
    return 0;
}
