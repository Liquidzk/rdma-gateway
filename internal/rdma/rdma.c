#include "rdma.h"

#include <rdma/rdma_cma.h>
#include <infiniband/verbs.h>

#include <errno.h>
#include <inttypes.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct rdma_listener {
    struct rdma_event_channel *ec;
    struct rdma_cm_id *id;
};

struct rdma_conn {
    struct rdma_event_channel *ec;
    struct rdma_cm_id *id;
    struct ibv_pd *pd;
    struct ibv_cq *cq;
};

struct rdma_mr {
    void *buf;
    size_t size;
    struct ibv_mr *mr;
};

static __thread char g_last_err[256];

static void set_err(const char *msg) {
    if (!msg) {
        snprintf(g_last_err, sizeof(g_last_err), "unknown error");
        return;
    }
    snprintf(g_last_err, sizeof(g_last_err), "%s", msg);
}

static void set_err_errno(const char *prefix) {
    if (!prefix) {
        prefix = "error";
    }
    snprintf(g_last_err, sizeof(g_last_err), "%s: %s", prefix, strerror(errno));
}

const char *rdma_last_error(void) {
    return g_last_err;
}

static int resolve_addr(const char *ip, int port, struct sockaddr **out_sa, socklen_t *out_len) {
    struct addrinfo hints;
    struct addrinfo *res = NULL;
    char port_str[16];

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    snprintf(port_str, sizeof(port_str), "%d", port);
    if (getaddrinfo(ip, port_str, &hints, &res) != 0 || res == NULL) {
        set_err("getaddrinfo failed");
        return -1;
    }

    *out_sa = (struct sockaddr *)malloc(res->ai_addrlen);
    if (!*out_sa) {
        freeaddrinfo(res);
        set_err("malloc failed");
        return -1;
    }
    memcpy(*out_sa, res->ai_addr, res->ai_addrlen);
    *out_len = (socklen_t)res->ai_addrlen;
    freeaddrinfo(res);
    return 0;
}

static int wait_event(struct rdma_event_channel *ec, enum rdma_cm_event_type expect, struct rdma_cm_event **out_event) {
    struct rdma_cm_event *event = NULL;
    if (rdma_get_cm_event(ec, &event)) {
        set_err_errno("rdma_get_cm_event failed");
        return -1;
    }
    if (event->event != expect) {
        snprintf(g_last_err, sizeof(g_last_err), "unexpected cm event %d (expected %d)", event->event, expect);
        rdma_ack_cm_event(event);
        return -1;
    }
    *out_event = event;
    return 0;
}

static int setup_qp(struct rdma_cm_id *id, struct ibv_pd **out_pd, struct ibv_cq **out_cq) {
    struct ibv_pd *pd = ibv_alloc_pd(id->verbs);
    if (!pd) {
        set_err_errno("ibv_alloc_pd failed");
        return -1;
    }

    struct ibv_cq *cq = ibv_create_cq(id->verbs, 32, NULL, NULL, 0);
    if (!cq) {
        ibv_dealloc_pd(pd);
        set_err_errno("ibv_create_cq failed");
        return -1;
    }

    struct ibv_qp_init_attr qp_attr;
    memset(&qp_attr, 0, sizeof(qp_attr));
    qp_attr.send_cq = cq;
    qp_attr.recv_cq = cq;
    qp_attr.qp_type = IBV_QPT_RC;
    qp_attr.cap.max_send_wr = 64;
    qp_attr.cap.max_recv_wr = 64;
    qp_attr.cap.max_send_sge = 1;
    qp_attr.cap.max_recv_sge = 1;
    qp_attr.sq_sig_all = 1;

    if (rdma_create_qp(id, pd, &qp_attr)) {
        ibv_destroy_cq(cq);
        ibv_dealloc_pd(pd);
        set_err_errno("rdma_create_qp failed");
        return -1;
    }

    *out_pd = pd;
    *out_cq = cq;
    return 0;
}

int rdma_listen_create(const char *bind_ip, int port, struct rdma_listener **out) {
    struct sockaddr *sa = NULL;
    socklen_t sa_len = 0;

    if (resolve_addr(bind_ip, port, &sa, &sa_len) != 0) {
        return -1;
    }

    struct rdma_event_channel *ec = rdma_create_event_channel();
    if (!ec) {
        free(sa);
        set_err_errno("rdma_create_event_channel failed");
        return -1;
    }

    struct rdma_cm_id *id = NULL;
    if (rdma_create_id(ec, &id, NULL, RDMA_PS_TCP)) {
        rdma_destroy_event_channel(ec);
        free(sa);
        set_err_errno("rdma_create_id failed");
        return -1;
    }

    if (rdma_bind_addr(id, sa)) {
        rdma_destroy_id(id);
        rdma_destroy_event_channel(ec);
        free(sa);
        set_err_errno("rdma_bind_addr failed");
        return -1;
    }

    if (rdma_listen(id, 1)) {
        rdma_destroy_id(id);
        rdma_destroy_event_channel(ec);
        free(sa);
        set_err_errno("rdma_listen failed");
        return -1;
    }

    free(sa);

    struct rdma_listener *listener = (struct rdma_listener *)calloc(1, sizeof(*listener));
    if (!listener) {
        rdma_destroy_id(id);
        rdma_destroy_event_channel(ec);
        set_err("calloc failed");
        return -1;
    }
    listener->ec = ec;
    listener->id = id;
    *out = listener;
    return 0;
}

int rdma_listen_accept(struct rdma_listener *listener, struct rdma_conn **out) {
    if (!listener || !out) {
        set_err("invalid listener");
        return -1;
    }

    struct rdma_cm_event *event = NULL;
    if (wait_event(listener->ec, RDMA_CM_EVENT_CONNECT_REQUEST, &event) != 0) {
        return -1;
    }

    struct rdma_cm_id *id = event->id;
    rdma_ack_cm_event(event);

    struct rdma_event_channel *conn_ec = rdma_create_event_channel();
    if (!conn_ec) {
        set_err_errno("rdma_create_event_channel failed");
        return -1;
    }

    if (rdma_migrate_id(id, conn_ec)) {
        rdma_destroy_event_channel(conn_ec);
        set_err_errno("rdma_migrate_id failed");
        return -1;
    }

    struct ibv_pd *pd = NULL;
    struct ibv_cq *cq = NULL;
    if (setup_qp(id, &pd, &cq) != 0) {
        rdma_destroy_event_channel(conn_ec);
        return -1;
    }

    struct rdma_conn_param param;
    memset(&param, 0, sizeof(param));
    param.initiator_depth = 1;
    param.responder_resources = 1;
    param.retry_count = 7;

    if (rdma_accept(id, &param)) {
        rdma_destroy_qp(id);
        ibv_destroy_cq(cq);
        ibv_dealloc_pd(pd);
        rdma_destroy_event_channel(conn_ec);
        set_err_errno("rdma_accept failed");
        return -1;
    }

    struct rdma_cm_event *est = NULL;
    if (wait_event(conn_ec, RDMA_CM_EVENT_ESTABLISHED, &est) != 0) {
        rdma_destroy_qp(id);
        ibv_destroy_cq(cq);
        ibv_dealloc_pd(pd);
        rdma_destroy_event_channel(conn_ec);
        return -1;
    }
    rdma_ack_cm_event(est);

    struct rdma_conn *conn = (struct rdma_conn *)calloc(1, sizeof(*conn));
    if (!conn) {
        rdma_destroy_qp(id);
        ibv_destroy_cq(cq);
        ibv_dealloc_pd(pd);
        rdma_destroy_event_channel(conn_ec);
        set_err("calloc failed");
        return -1;
    }

    conn->ec = conn_ec;
    conn->id = id;
    conn->pd = pd;
    conn->cq = cq;
    *out = conn;
    return 0;
}

int rdma_listen_close(struct rdma_listener *listener) {
    if (!listener) {
        return 0;
    }
    if (listener->id) {
        rdma_destroy_id(listener->id);
    }
    if (listener->ec) {
        rdma_destroy_event_channel(listener->ec);
    }
    free(listener);
    return 0;
}

int rdma_dial(const char *server_ip, int port, struct rdma_conn **out) {
    struct sockaddr *sa = NULL;
    socklen_t sa_len = 0;

    if (resolve_addr(server_ip, port, &sa, &sa_len) != 0) {
        return -1;
    }

    struct rdma_event_channel *ec = rdma_create_event_channel();
    if (!ec) {
        free(sa);
        set_err_errno("rdma_create_event_channel failed");
        return -1;
    }

    struct rdma_cm_id *id = NULL;
    if (rdma_create_id(ec, &id, NULL, RDMA_PS_TCP)) {
        rdma_destroy_event_channel(ec);
        free(sa);
        set_err_errno("rdma_create_id failed");
        return -1;
    }

    if (rdma_resolve_addr(id, NULL, sa, 2000)) {
        rdma_destroy_id(id);
        rdma_destroy_event_channel(ec);
        free(sa);
        set_err_errno("rdma_resolve_addr failed");
        return -1;
    }

    free(sa);

    struct rdma_cm_event *event = NULL;
    if (wait_event(ec, RDMA_CM_EVENT_ADDR_RESOLVED, &event) != 0) {
        rdma_destroy_id(id);
        rdma_destroy_event_channel(ec);
        return -1;
    }
    rdma_ack_cm_event(event);

    if (rdma_resolve_route(id, 2000)) {
        rdma_destroy_id(id);
        rdma_destroy_event_channel(ec);
        set_err_errno("rdma_resolve_route failed");
        return -1;
    }

    if (wait_event(ec, RDMA_CM_EVENT_ROUTE_RESOLVED, &event) != 0) {
        rdma_destroy_id(id);
        rdma_destroy_event_channel(ec);
        return -1;
    }
    rdma_ack_cm_event(event);

    struct ibv_pd *pd = NULL;
    struct ibv_cq *cq = NULL;
    if (setup_qp(id, &pd, &cq) != 0) {
        rdma_destroy_id(id);
        rdma_destroy_event_channel(ec);
        return -1;
    }

    struct rdma_conn_param param;
    memset(&param, 0, sizeof(param));
    param.initiator_depth = 1;
    param.responder_resources = 1;
    param.retry_count = 7;

    if (rdma_connect(id, &param)) {
        rdma_destroy_qp(id);
        ibv_destroy_cq(cq);
        ibv_dealloc_pd(pd);
        rdma_destroy_id(id);
        rdma_destroy_event_channel(ec);
        set_err_errno("rdma_connect failed");
        return -1;
    }

    if (wait_event(ec, RDMA_CM_EVENT_ESTABLISHED, &event) != 0) {
        rdma_destroy_qp(id);
        ibv_destroy_cq(cq);
        ibv_dealloc_pd(pd);
        rdma_destroy_id(id);
        rdma_destroy_event_channel(ec);
        return -1;
    }
    rdma_ack_cm_event(event);

    struct rdma_conn *conn = (struct rdma_conn *)calloc(1, sizeof(*conn));
    if (!conn) {
        rdma_destroy_qp(id);
        ibv_destroy_cq(cq);
        ibv_dealloc_pd(pd);
        rdma_destroy_id(id);
        rdma_destroy_event_channel(ec);
        set_err("calloc failed");
        return -1;
    }

    conn->ec = ec;
    conn->id = id;
    conn->pd = pd;
    conn->cq = cq;
    *out = conn;
    return 0;
}

int rdma_conn_close(struct rdma_conn *conn) {
    if (!conn) {
        return 0;
    }
    if (conn->id) {
        rdma_disconnect(conn->id);
        rdma_destroy_qp(conn->id);
        rdma_destroy_id(conn->id);
    }
    if (conn->cq) {
        ibv_destroy_cq(conn->cq);
    }
    if (conn->pd) {
        ibv_dealloc_pd(conn->pd);
    }
    if (conn->ec) {
        rdma_destroy_event_channel(conn->ec);
    }
    free(conn);
    return 0;
}

int rdma_reg_mr(struct rdma_conn *conn, void *buf, size_t len, int access, struct ibv_mr **out) {
    if (!conn || !conn->pd || !buf || len == 0 || !out) {
        set_err("invalid args to rdma_reg_mr");
        return -1;
    }
    struct ibv_mr *mr = ibv_reg_mr(conn->pd, buf, len, access);
    if (!mr) {
        set_err_errno("ibv_reg_mr failed");
        return -1;
    }
    *out = mr;
    return 0;
}

int rdma_dereg_mr(struct ibv_mr *mr) {
    if (!mr) {
        return 0;
    }
    if (ibv_dereg_mr(mr)) {
        set_err_errno("ibv_dereg_mr failed");
        return -1;
    }
    return 0;
}

int rdma_post_recv(struct rdma_conn *conn, struct ibv_mr *mr, void *buf, size_t len) {
    if (!conn || !conn->id || !mr || !buf || len == 0) {
        set_err("invalid args to rdma_post_recv");
        return -1;
    }
    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)buf;
    sge.length = (uint32_t)len;
    sge.lkey = mr->lkey;

    struct ibv_recv_wr wr;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = (uintptr_t)buf;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    struct ibv_recv_wr *bad = NULL;
    if (ibv_post_recv(conn->id->qp, &wr, &bad)) {
        set_err_errno("ibv_post_recv failed");
        return -1;
    }
    return 0;
}

int rdma_post_send(struct rdma_conn *conn, struct ibv_mr *mr, void *buf, size_t len) {
    if (!conn || !conn->id || !mr || !buf || len == 0) {
        set_err("invalid args to rdma_post_send");
        return -1;
    }
    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)buf;
    sge.length = (uint32_t)len;
    sge.lkey = mr->lkey;

    struct ibv_send_wr wr;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = (uintptr_t)buf;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = IBV_SEND_SIGNALED;

    struct ibv_send_wr *bad = NULL;
    if (ibv_post_send(conn->id->qp, &wr, &bad)) {
        set_err_errno("ibv_post_send failed");
        return -1;
    }
    return 0;
}

int rdma_post_write(struct rdma_conn *conn, struct ibv_mr *mr, void *buf, size_t len, uint64_t remote_addr, uint32_t rkey) {
    if (!conn || !conn->id || !mr || !buf || len == 0) {
        set_err("invalid args to rdma_post_write");
        return -1;
    }
    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)buf;
    sge.length = (uint32_t)len;
    sge.lkey = mr->lkey;

    struct ibv_send_wr wr;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = (uintptr_t)buf;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_WRITE;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = remote_addr;
    wr.wr.rdma.rkey = rkey;

    struct ibv_send_wr *bad = NULL;
    if (ibv_post_send(conn->id->qp, &wr, &bad)) {
        set_err_errno("ibv_post_send failed");
        return -1;
    }
    return 0;
}

int rdma_post_read(struct rdma_conn *conn, struct ibv_mr *mr, void *buf, size_t len, uint64_t remote_addr, uint32_t rkey) {
    if (!conn || !conn->id || !mr || !buf || len == 0) {
        set_err("invalid args to rdma_post_read");
        return -1;
    }
    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)buf;
    sge.length = (uint32_t)len;
    sge.lkey = mr->lkey;

    struct ibv_send_wr wr;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = (uintptr_t)buf;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_READ;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = remote_addr;
    wr.wr.rdma.rkey = rkey;

    struct ibv_send_wr *bad = NULL;
    if (ibv_post_send(conn->id->qp, &wr, &bad)) {
        set_err_errno("ibv_post_send failed");
        return -1;
    }
    return 0;
}

int rdma_poll(struct rdma_conn *conn, int *out_opcode, size_t *out_len, uint64_t *out_wr_id) {
    if (!conn || !conn->cq || !out_opcode || !out_len || !out_wr_id) {
        set_err("invalid args to rdma_poll");
        return -1;
    }
    struct ibv_wc wc;
    memset(&wc, 0, sizeof(wc));
    int n = ibv_poll_cq(conn->cq, 1, &wc);
    if (n < 0) {
        set_err_errno("ibv_poll_cq failed");
        return -1;
    }
    if (n == 0) {
        return 1;
    }
    if (wc.status != IBV_WC_SUCCESS) {
        snprintf(g_last_err, sizeof(g_last_err), "wc error status=%d", wc.status);
        return -1;
    }
    *out_opcode = wc.opcode;
    *out_len = wc.byte_len;
    *out_wr_id = wc.wr_id;
    return 0;
}

int rdma_alloc_and_reg_mr(struct rdma_conn *conn, size_t len, int access, struct rdma_mr **out) {
    if (!conn || !conn->pd || len == 0 || !out) {
        set_err("invalid args to rdma_alloc_and_reg_mr");
        return -1;
    }
    void *buf = NULL;
    int rc = posix_memalign(&buf, 4096, len);
    if (rc != 0 || !buf) {
        set_err("posix_memalign failed");
        return -1;
    }
    memset(buf, 0, len);

    struct ibv_mr *mr = ibv_reg_mr(conn->pd, buf, len, access);
    if (!mr) {
        free(buf);
        set_err_errno("ibv_reg_mr failed");
        return -1;
    }

    struct rdma_mr *wrap = (struct rdma_mr *)calloc(1, sizeof(*wrap));
    if (!wrap) {
        ibv_dereg_mr(mr);
        free(buf);
        set_err("calloc failed");
        return -1;
    }
    wrap->buf = buf;
    wrap->size = len;
    wrap->mr = mr;
    *out = wrap;
    return 0;
}

int rdma_mr_free(struct rdma_mr *mr) {
    if (!mr) {
        return 0;
    }
    if (mr->mr) {
        if (ibv_dereg_mr(mr->mr)) {
            set_err_errno("ibv_dereg_mr failed");
            return -1;
        }
    }
    if (mr->buf) {
        free(mr->buf);
    }
    free(mr);
    return 0;
}

void *rdma_mr_addr(struct rdma_mr *mr) {
    if (!mr) {
        return NULL;
    }
    return mr->buf;
}

uint32_t rdma_mr_rkey(struct rdma_mr *mr) {
    if (!mr || !mr->mr) {
        return 0;
    }
    return mr->mr->rkey;
}

size_t rdma_mr_size(struct rdma_mr *mr) {
    if (!mr) {
        return 0;
    }
    return mr->size;
}
