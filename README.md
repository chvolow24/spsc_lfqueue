A simple single-producer, single-consumer lock-free queue using `<stdatomic.h>`

The queue must be initialized with `lfqueue_init` and deinitialized with `lfqueue_deinit`

`try` and `wait` versions of both enqueue and dequeue operations are provided. 

The `try` versions always return immediately with either `LFQUEUE_SUCCESS` or an error code, indicating that the queue is empty or full. 

The `wait` versions poll the queue on a loop (with user-specified iteration sleep time) until the operation is possible. If the optional `_Atomic bool *cancel_opt` argument is provided, the underlying boolean is checked on each iteration; if it becomes true, the operation is canceled.
