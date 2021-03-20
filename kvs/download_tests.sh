download_test() {
    curl "https://raw.githubusercontent.com/pingcap/talent-plan/master/courses/rust/projects/project-5/tests/$1.rs" > "tests/$1.rs"
    echo "Downloaded $1.rs"
}

download_test "kv_store"
download_test "thread_pool"