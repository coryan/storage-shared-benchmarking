import maxt;
#include <iostream>
#include <stdexcept>

int main(int argc, char* argv[]) try {
    maxt::parse_args(argc, argv);
    return EXIT_SUCCESS;
} catch(std::exception const& ex) {
    std::cerr << "Standard exception raised: " << ex.what() << "\n";
    return EXIT_FAILURE;
}
