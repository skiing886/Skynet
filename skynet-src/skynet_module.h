#ifndef SKYNET_MODULE_H
#define SKYNET_MODULE_H

struct skynet_context;

typedef void * (*skynet_dl_create)(void);
typedef int (*skynet_dl_init)(void * inst, struct skynet_context *, const char * parm);
typedef void (*skynet_dl_release)(void * inst);

struct skynet_module {
	const char * name; //xxx 模块的名称 master gate etc...
	void * module;// xxx 指向dlopen()函数打开的动态库的句柄
	skynet_dl_create create;
	skynet_dl_init init;
	skynet_dl_release release;
};

void skynet_module_insert(struct skynet_module *mod);
struct skynet_module * skynet_module_query(const char * name);
void * skynet_module_instance_create(struct skynet_module *);
int skynet_module_instance_init(struct skynet_module *, void * inst, struct skynet_context *ctx, const char * parm);
void skynet_module_instance_release(struct skynet_module *, void *inst);

void skynet_module_init(const char *path);

#endif
