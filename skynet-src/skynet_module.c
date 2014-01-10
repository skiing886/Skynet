#include "skynet_module.h"

#include <assert.h>
#include <string.h>
#include <dlfcn.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>

#define MAX_MODULE_TYPE 32

struct modules {
	int count;//xxx 当前已配置的服务数
	int lock;//xxx 用于线程同步的变量
	const char * path;//xxx 模块所在的路劲
	struct skynet_module m[MAX_MODULE_TYPE];//xxx 最多可以加载的服务数
};

static struct modules * M = NULL;


/**
@brief   打开一个动态库文件，改动态库文件
		 将作为一项独立的服务模块
		 client.so、gate.so、harbor.so etc
@param   m 模块管理器  
@param   name 待打开的模块的名称 
@return  已动态库形式打开的模块的句柄
*/
static void *
_try_open(struct modules *m, const char * name) {
	const char * path = m->path;
	size_t path_size = strlen(path);
	size_t name_size = strlen(name);

	int sz = path_size + name_size;
	char tmp[sz];
	int i;
	for (i=0;path[i]!='?' && path[i]!='\0';i++) {
		tmp[i] = path[i];
	}
	memcpy(tmp+i,name,name_size); //xxx 一个完整的服务的路径 ./service/master.so
	if (path[i] == '?') {
		strcpy(tmp+i+name_size,path+i+1);//xxx 拷贝.so
	} else {
		fprintf(stderr,"Invalid C service path\n");
		exit(1);
	}

	void * dl = dlopen(tmp, RTLD_NOW | RTLD_GLOBAL);//xxx 该系统调用加载一个.so文件到内存并返回一个句柄

	if (dl == NULL) {
		fprintf(stderr, "try open %s failed : %s\n",tmp,dlerror());
	}

	return dl;
}

static struct skynet_module * 
_query(const char * name) {
	int i;
	for (i=0;i<M->count;i++) {
		if (strcmp(M->m[i].name,name)==0) {
			return &M->m[i];
		}
	}
	return NULL;
}


/**
@brief   获取服务模块三个函数的指针
		 module_create()、module_init()、module_release()
@param   mod 服务模块  
@return  0--成功  其他--失败
*/
static int
_open_sym(struct skynet_module *mod) {
	size_t name_size = strlen(mod->name);
	char tmp[name_size + 9]; // create/init/release , longest name is release (7)
	memcpy(tmp, mod->name, name_size);
	strcpy(tmp+name_size, "_create");
	mod->create = dlsym(mod->module, tmp);//xxx 导出master.so中master_create()函数的地址
	strcpy(tmp+name_size, "_init");
	mod->init = dlsym(mod->module, tmp);
	strcpy(tmp+name_size, "_release");
	mod->release = dlsym(mod->module, tmp);

	return mod->init == NULL;//xxx 
}

/**
@brief   查询一个服务是否已经启动
		 如果没有启动，尝试重启该服务
		 获取服务模块三个函数的指针
		 并导出服务的.so内的三个特征函数
@param   name 服务名称 
@return  0--成功  其他--失败
*/
struct skynet_module * 
skynet_module_query(const char * name) {
	struct skynet_module * result = _query(name);
	if (result)
		return result;

	while(__sync_lock_test_and_set(&M->lock,1)) {}

	result = _query(name); // double check

	if (result == NULL && M->count < MAX_MODULE_TYPE) {
		int index = M->count;
		void * dl = _try_open(M,name);
		if (dl) {
			M->m[index].name = name;
			M->m[index].module = dl;

			if (_open_sym(&M->m[index]) == 0) {//xxx 至少module_init()函数要被正确导出了
				M->m[index].name = strdup(name);
				M->count ++;
				result = &M->m[index];
			}
		}
	}

	__sync_lock_release(&M->lock);

	return result;
}

void 
skynet_module_insert(struct skynet_module *mod) {
	while(__sync_lock_test_and_set(&M->lock,1)) {}

	struct skynet_module * m = _query(mod->name);
	assert(m == NULL && M->count < MAX_MODULE_TYPE);
	int index = M->count;
	M->m[index] = *mod;
	++M->count;
	__sync_lock_release(&M->lock);
}

/**
@breif 初始化服务，即调用服务内部的create()函数
	   如logger_create()会创建一个struct logger
	   的对象，并初始化其成员
@param m 特定的服务
@return void* 
*/
void * 
skynet_module_instance_create(struct skynet_module *m) {
	if (m->create) {
		return m->create();//xxx 调用模块的module_create()函数
	} else {
		return (void *)(intptr_t)(~0);
	}
}

/**
@breif 开始运行一个服务
@param m 特定的服务
@param inst 服务内部的结构实例
@param ctx 服务的ctx
@param param 传给服务内部的参数
@return 服务内部调用xxx_init()返回 
*/
int
skynet_module_instance_init(struct skynet_module *m, void * inst, struct skynet_context *ctx, const char * parm) {
	return m->init(inst, ctx, parm);
}

void 
skynet_module_instance_release(struct skynet_module *m, void *inst) {
	if (m->release) {
		m->release(inst);
	}
}

void 
skynet_module_init(const char *path) {
	struct modules *m = malloc(sizeof(*m));
	m->count = 0;
	m->path = strdup(path);
	m->lock = 0;

	M = m;
}
