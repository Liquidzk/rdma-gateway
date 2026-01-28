#ifndef RDMA_WRAPPER_H
#define RDMA_WRAPPER_H

#include <stdint.h>

struct rdma_listener;
struct rdma_conn;
struct rdma_mr;

// Returns 0 on success, -1 on error. On error, call rdma_last_error().
int rdma_listen_create(const char *bind_ip, int port, struct rdma_listener **out);
int rdma_listen_accept(struct rdma_listener *listener, struct rdma_conn **out);
int rdma_listen_close(struct rdma_listener *listener);

int rdma_dial(const char *server_ip, int port, struct rdma_conn **out);
int rdma_conn_close(struct rdma_conn *conn);

int rdma_reg_mr(struct rdma_conn *conn, void *buf, size_t len, int access, struct ibv_mr **out);
int rdma_dereg_mr(struct ibv_mr *mr);
int rdma_post_recv(struct rdma_conn *conn, struct ibv_mr *mr, void *buf, size_t len);
int rdma_post_send(struct rdma_conn *conn, struct ibv_mr *mr, void *buf, size_t len);
int rdma_post_write(struct rdma_conn *conn, struct ibv_mr *mr, void *buf, size_t len, uint64_t remote_addr, uint32_t rkey);
int rdma_post_read(struct rdma_conn *conn, struct ibv_mr *mr, void *buf, size_t len, uint64_t remote_addr, uint32_t rkey);
// Returns 0 with a completion, 1 if no completion, -1 on error.
int rdma_poll(struct rdma_conn *conn, int *out_opcode, size_t *out_len, uint64_t *out_wr_id);

int rdma_alloc_and_reg_mr(struct rdma_conn *conn, size_t len, int access, struct rdma_mr **out);
int rdma_mr_free(struct rdma_mr *mr);
void *rdma_mr_addr(struct rdma_mr *mr);
uint32_t rdma_mr_rkey(struct rdma_mr *mr);
size_t rdma_mr_size(struct rdma_mr *mr);

const char *rdma_last_error(void);

#endif
