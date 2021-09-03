#include <iostream>
#include <type_traits>
#include <vector>
#include <memory>

void print_label(const std::string &label = "", bool one_line = false) {
    if (label == "") return;

    if (one_line) { std::cout << label << " = "; }
    else { std::cout << "[ " << label << " ]" << "\n"; }
}

template <typename T>
void print(std::shared_ptr<T> p, const std::string &label = "") {
    print_label(label);
    std::cout << p->ToString() << "\n" << std::endl;
}

template <typename T>
void print(const T &t, const std::string &label = "") {
    bool one_line = std::is_arithmetic<T>();
    print_label(label, one_line);
    // std::cout << t << "\n" << std::endl;
    std::cout << t << std::endl;
}

template <typename T>
void print(const std::vector<T> &v, const std::string &label = "") {
    print_label(label);
    for (auto &i : v) {
        std::cout << i << " ";
    }
    std::cout << "\n" << std::endl;
}

// template <typename T, size_t N>
template <typename T, int64_t N>
void print(T (&arr)[N]) {
    if (typeid(T).name() == typeid('c').name()) {
        // c_str
        std::cout << arr << std::endl;
        return;
    }
    for (auto &i : arr) {
        std::cout << i << " ";
    }
    std::cout << "\n" << std::endl;
}