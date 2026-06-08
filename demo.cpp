#include "demo.h"

void DemoMd::on_md(const MDUniOrder& order){
    std::cout << "on_md: " << order.instrumentId << " " << order.price << " " << order.qty << std::endl;
}