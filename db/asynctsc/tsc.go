package asynctsc

/*
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sched.h>
#include "taos.h"
#include <pthread.h>
#include <sys/prctl.h>
#include <semaphore.h>
#include <stdbool.h>

typedef struct query_param {
    sem_t sem;
    int code;
    const char* err;
    TAOS_RES *pRequest;
    void *userParam;
} query_param;

void query_callback(void *param, TAOS_RES *res, int code) {
    query_param *pParam = param;
    pParam->code = code;
    if (0 != code) {
        pParam->err = taos_errstr(res);
    }
    pParam->pRequest = res;
    int post_code = sem_post(&pParam->sem);
    if (0 != post_code) {
        // EINVAL and EOVERFLOW are not expected
        fprintf(stderr, "sem_post failed, code:%d\n", post_code);
        abort();
    }
}

typedef enum {
    TAOSA_QUERY = 1,
    TAOSA_FREE = 2,
} Query_EVENT;

typedef struct query_thread {
    Query_EVENT event;
    int shutdown;
    void *task;
    pthread_mutex_t lock;
    pthread_cond_t notify;
    pthread_t thread;
    query_param *query_param;
} query_thread;

typedef struct taosa_query_result {
    int code;
    const char *err;
    TAOS_RES *res;
    bool is_update_query;
    int64_t affected_rows;
    int32_t field_count;
    TAOS_FIELD *field;
    int precision;
} taosa_query_result;

typedef void (*adapter_query_cb)(void *param, taosa_query_result *result);

typedef void (*adapter_free_cb)(void *param);

typedef struct query_task {
    TAOS *conn;
    char *sql;
    int64_t req_id;
    adapter_query_cb cb;
    void *param;
} query_task;

typedef struct free_task {
    TAOS_RES *res;
    adapter_free_cb cb;
    void *param;
} free_task;

void *worker_tsc(void *arg) {
    prctl(PR_SET_NAME, "tsc_a");
    query_thread *thread_info = (query_thread *) arg;
    while (1) {
        pthread_mutex_lock(&thread_info->lock);
        while (thread_info->task == NULL && !thread_info->shutdown) {
            pthread_cond_wait(&thread_info->notify, &thread_info->lock);
        }
        if (thread_info->shutdown) break;
        switch (thread_info->event) {
            case TAOSA_QUERY: {
                const query_task *task = (query_task *) thread_info->task;
                thread_info->query_param->userParam = task->param;
                taos_query_a_with_reqid(task->conn, task->sql, query_callback, thread_info->query_param,task->req_id);
                int ret = 0;
                do {
                    ret = sem_wait(&thread_info->query_param->sem);
                } while (-1 == ret && errno == EINTR);
                if (ret != 0) {
                    fprintf(stderr, "sem_wait failed, code:%d\n", ret);
                    abort();
                }
                if (thread_info->query_param->code != 0) {
                    taosa_query_result result;
                    result.code = thread_info->query_param->code;
                    result.err = thread_info->query_param->err;
                    task->cb(task->param, &result);
                    if (thread_info->query_param->pRequest != NULL) {
                        taos_free_result(thread_info->query_param->pRequest);
						thread_info->query_param->pRequest = NULL;
                        result.res = NULL;
                    }
                } else {
                    taosa_query_result result;
                    result.code = 0;
                    result.err = NULL;
                    result.res = thread_info->query_param->pRequest;
                    result.is_update_query = taos_is_update_query(result.res);
                    if (result.is_update_query) {
                        result.affected_rows = taos_affected_rows64(result.res);
                        taos_free_result(result.res);
                        result.res = NULL;
                    }else {
                        result.field_count = taos_field_count(result.res);
                        result.field = taos_fetch_fields(result.res);
                        result.precision = taos_result_precision(result.res);
                    }
                    task->cb(task->param, &result);
                }
                break;
            }
            case TAOSA_FREE: {
                free_task *task = (free_task *) thread_info->task;
                taos_free_result(task->res);
                task->cb(task->param);
                break;
            }
            default:
                break;
        }
        if (thread_info->task != NULL) {
            free(thread_info->task);
            thread_info->task = NULL;
        }
        pthread_mutex_unlock(&thread_info->lock);
    }
    pthread_mutex_unlock(&thread_info->lock);

    pthread_exit(NULL);
}

#define TAOSA_SYSTEM_ERROR(code)             (0x80ff0000 | (code))

int init_query_thread(query_thread **thread_info_p) {
    query_thread *thread_info = (query_thread *) malloc(sizeof(query_thread));
    if (thread_info == NULL) {
        return -1;
    }
    thread_info->event = 0;
    thread_info->shutdown = 0;
    thread_info->task = NULL;
    int code = pthread_mutex_init(&thread_info->lock, NULL);
    if (code != 0) {
        free(thread_info);
        return TAOSA_SYSTEM_ERROR(code);
    }
    code = pthread_cond_init(&thread_info->notify, NULL);
    if (code != 0) {
        pthread_mutex_destroy(&thread_info->lock);
        free(thread_info);
        return TAOSA_SYSTEM_ERROR(code);
    }
    code = pthread_create(&thread_info->thread, NULL, worker_tsc, (void *) thread_info);
    if (code != 0) {
        pthread_mutex_destroy(&thread_info->lock);
        pthread_cond_destroy(&thread_info->notify);
        free(thread_info);
        return TAOSA_SYSTEM_ERROR(code);
    }
    thread_info->query_param = (query_param *) malloc(sizeof(query_param));
    if (thread_info->query_param == NULL) {
        pthread_mutex_destroy(&thread_info->lock);
        pthread_cond_destroy(&thread_info->notify);
        free(thread_info);
        return -1;
    }
    code = sem_init(&thread_info->query_param->sem, 0, 0);
    if (code != 0) {
        pthread_mutex_destroy(&thread_info->lock);
        pthread_cond_destroy(&thread_info->notify);
        free(thread_info->query_param);
        free(thread_info);
        return TAOSA_SYSTEM_ERROR(code);
    }
    // cpu_set_t cpuset;
    // CPU_ZERO(&cpuset);
    // CPU_SET(24, &cpuset);

    //   int result = pthread_setaffinity_np(thread_info->thread, sizeof(cpu_set_t), &cpuset);
    //   if (result != 0) {
    //       perror("pthread_setaffinity_np");
    //   }
    *thread_info_p = thread_info;
    return 0;
}

void destroy_thread(query_thread *thread_info) {
    if (thread_info == NULL || thread_info->shutdown) return;
    thread_info->shutdown = 1;
    pthread_cond_signal(&thread_info->notify);
    pthread_join(thread_info->thread, NULL);
    pthread_mutex_destroy(&thread_info->lock);
    pthread_cond_destroy(&thread_info->notify);
    if (thread_info->task != NULL) free(thread_info->task);
    if (thread_info->query_param != NULL) free(thread_info->query_param);
    free(thread_info);
}

extern void AdapterQueryCallback(void *param, taosa_query_result *result);

extern void AdapterFreeCallback(void *param);

void adapter_query_a_wrapper(query_thread *thread_info, TAOS *conn, char *sql, void *param, int64_t req_id) {
    query_task *task = (query_task *) malloc(sizeof(query_task));
    task->conn = conn;
    task->sql = sql;
    task->cb = AdapterQueryCallback;
    task->param = param;
    task->req_id = req_id;
    pthread_mutex_lock(&thread_info->lock);
    thread_info->event = TAOSA_QUERY;
    thread_info->task = task;
    pthread_cond_signal(&thread_info->notify);
    pthread_mutex_unlock(&thread_info->lock);
}

void adapter_free_a_wrapper(query_thread *thread_info, TAOS_RES *res, void *param) {
    free_task *task = (free_task *) malloc(sizeof(free_task));
    task->res = res;
    task->cb = AdapterFreeCallback;
    task->param = param;
    pthread_mutex_lock(&thread_info->lock);
    thread_info->event = TAOSA_FREE;
    thread_info->task = task;
    pthread_cond_signal(&thread_info->notify);
    pthread_mutex_unlock(&thread_info->lock);
}

*/
import "C"
import (
	"errors"
	"fmt"
	"unsafe"

	"github.com/taosdata/taosadapter/v3/driver/wrapper/cgo"
)

func CreateThread() (unsafe.Pointer, error) {
	var cThread *C.struct_query_thread
	ret := C.init_query_thread(&cThread)
	if ret != 0 {
		if ret == -1 {
			return nil, errors.New("malloc failed")
		}
		errCode := uint32(ret) & 0x0000FFFF
		return nil, fmt.Errorf("system error code: %d", errCode)
	}
	return unsafe.Pointer(cThread), nil
}

func DestroyThread(thread unsafe.Pointer) {
	C.destroy_thread((*C.query_thread)(thread))
}

func AdapterQueryAWithReqID(thread unsafe.Pointer, conn unsafe.Pointer, sql string, handler cgo.Handle, reqID int64) {
	cSql := C.CString(sql)
	//defer C.free(unsafe.Pointer(cSql))
	C.adapter_query_a_wrapper((*C.query_thread)(thread), conn, (*C.char)(cSql), handler.Pointer(), C.int64_t(reqID))
}

func AdapterFreeResult(thread unsafe.Pointer, res unsafe.Pointer, handler cgo.Handle) {
	C.adapter_free_a_wrapper((*C.query_thread)(thread), res, handler.Pointer())
}
