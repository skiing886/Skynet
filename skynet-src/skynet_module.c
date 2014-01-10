#include "skynet_module.h"

#include <assert.h>
#include <string.h>
#include <dlfcn.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>

#define MAX_MODULE_TYPE 32

struct modules {
	int count;//xxx ��ǰ�����õķ�����
	int lock;//xxx �����߳�ͬ���ı���
	const char * path;//xxx ģ�����ڵ�·��
	struct skynet_module m[MAX_MODULE_TYPE];//xxx �����Լ��صķ�����
};

static struct modules * M = NULL;


/**
@brief   ��һ����̬���ļ����Ķ�̬���ļ�
		 ����Ϊһ������ķ���ģ��
		 client.so��gate.so��harbor.so etc
@param   m ģ�������  
@param   name ���򿪵�ģ������� 
@return  �Ѷ�̬����ʽ�򿪵�ģ��ľ��
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
	memcpy(tmp+i,name,name_size); //xxx һ�������ķ����·�� ./service/master.so
	if (path[i] == '?') {
		strcpy(tmp+i+name_size,path+i+1);//xxx ����.so
	} else {
		fprintf(stderr,"Invalid C service path\n");
		exit(1);
	}

	void * dl = dlopen(tmp, RTLD_NOW | RTLD_GLOBAL);//xxx ��ϵͳ���ü���һ��.so�ļ����ڴ沢����һ�����

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
@brief   ��ȡ����ģ������������ָ��
		 module_create()��module_init()��module_release()
@param   mod ����ģ��  
@return  0--�ɹ�  ����--ʧ��
*/
static int
_open_sym(struct skynet_module *mod) {
	size_t name_size = strlen(mod->name);
	char tmp[name_size + 9]; // create/init/release , longest name is release (7)
	memcpy(tmp, mod->name, name_size);
	strcpy(tmp+name_size, "_create");
	mod->create = dlsym(mod->module, tmp);//xxx ����master.so��master_create()�����ĵ�ַ
	strcpy(tmp+name_size, "_init");
	mod->init = dlsym(mod->module, tmp);
	strcpy(tmp+name_size, "_release");
	mod->release = dlsym(mod->module, tmp);

	return mod->init == NULL;//xxx 
}

/**
@brief   ��ѯһ�������Ƿ��Ѿ�����
		 ���û�����������������÷���
		 ��ȡ����ģ������������ָ��
		 �����������.so�ڵ�������������
@param   name �������� 
@return  0--�ɹ�  ����--ʧ��
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

			if (_open_sym(&M->m[index]) == 0) {//xxx ����module_init()����Ҫ����ȷ������
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
@breif ��ʼ�����񣬼����÷����ڲ���create()����
	   ��logger_create()�ᴴ��һ��struct logger
	   �Ķ��󣬲���ʼ�����Ա
@param m �ض��ķ���
@return void* 
*/
void * 
skynet_module_instance_create(struct skynet_module *m) {
	if (m->create) {
		return m->create();//xxx ����ģ���module_create()����
	} else {
		return (void *)(intptr_t)(~0);
	}
}

/**
@breif ��ʼ����һ������
@param m �ض��ķ���
@param inst �����ڲ��Ľṹʵ��
@param ctx �����ctx
@param param ���������ڲ��Ĳ���
@return �����ڲ�����xxx_init()���� 
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
