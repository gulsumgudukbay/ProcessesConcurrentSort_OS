#include <linux/moduleparam.h>
#include <linux/init.h>
#include <linux/module.h>
#include <linux/kernel.h>
#include <linux/sched.h> //task_pid_nr
static int pid_given = 0;

/* This function is called when the module is loaded. */
int simple_init(void)
{
	struct task_struct *cur;

	#define next_task(p) \
	list_entry_rcu((p)->tasks.next, struct task_struct, tasks)

	#define for_each_process(p) \
	for (p = &init_task ; (p = next_task(p)) != &init_task ; )

	//task lock
	rcu_read_lock(); 

	//iterate over the processes                                                   
	for_each_process(cur) {                                             
		task_lock(cur);                                             
		if((int) task_pid_nr(cur) == pid_given){ //found the process with given id!
			printk(KERN_INFO "Loading Module\n");
			printk("The process name is: %s\n", cur->comm);
			printk("The process id is: %d\n", (int) task_pid_nr(cur));
			printk("The process state is: %ld\n", cur->state);
			printk("The process priority is: %ld\n", (long int) cur->prio);
			printk("The process parent id is: %d\n", (int) task_pid_nr(cur->parent));
			printk("The process group id is: %d\n", (int) task_tgid_nr(cur));
			printk("The process scheduling policy is: %d\n", cur->policy);
			printk("The process vid is: %d\n", (int) task_pid_vnr(cur));
			printk("The process terminal in which the process is running on is: %d\n", cur->signal->tty);
			printk("The process user memory is: %d\n", cur->active_mm);
			printk("\n");
		}
		task_unlock(cur);                                           
	}                                                                    
	rcu_read_unlock();       

	//return -1; //debug mode of working
	return 0;
}

/* This function is called when the module is removed. */
void simple_exit(void) {
	printk(KERN_INFO "Removing Module\n");
}

/* Macros for registering module entry and exit points. */
module_init( simple_init );
module_exit( simple_exit );

MODULE_LICENSE("GPL");
MODULE_DESCRIPTION("Simple Module");
MODULE_AUTHOR("GULSUM GUDUKBAY");
module_param(pid_given, int, 0);
