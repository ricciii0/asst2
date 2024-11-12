#include "tasksys.h"

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    this->num_threads_=num_threads;
    threads_pool_=new std::thread[num_threads];
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {
    delete []threads_pool_;
}

void TaskSystemParallelSpawn::thread_run(IRunnable* runnable,int num_total_tasks,std::mutex* mutex,int* curr_task){
    int turn=-1;     
    while (turn<num_total_tasks)
    {
        mutex->lock();
        turn=*curr_task;
        *curr_task+=1;
        mutex->unlock();
        if(turn>=num_total_tasks){
            break;
        }
        runnable->runTask(turn,num_total_tasks);
    }
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    std::mutex* mutex=new std::mutex;
    int* curr_task=new int;
    *curr_task=0;
    for(int i=0;i<num_threads_;i++){
        threads_pool_[i]=std::thread(&TaskSystemParallelSpawn::thread_run,this,runnable,num_total_tasks,mutex,curr_task);
    }
    for(int i=0;i<num_threads_;i++){
        threads_pool_[i].join();
    }
    delete mutex;
    delete curr_task;
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}
/*
 * ================================================================
 * Task State Implementation
 * ================================================================
 */

TaskState::TaskState(){
    mutex_=new std::mutex();
    finished_=new std::condition_variable();
    finished_mutex_=new std::mutex();
    runnable_=nullptr;
    finished_tasks_=-1;
    left_tasks_=-1;
    num_total_tasks_=-1;
}
TaskState::~TaskState(){
    delete mutex_;
    delete finished_;
    delete finished_mutex_;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    num_threads_=num_threads;
    killed=false;
    state=new TaskState();
    threads_pool_=new std::thread[num_threads_];
    for (int i = 0; i < num_threads_; i++)
    {
        threads_pool_[i]=std::thread(&TaskSystemParallelThreadPoolSpinning::spin,this);
    }
    
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    killed=true;
    for(int i=0;i<num_threads_;i++){
        threads_pool_[i].join();
    }
    delete[]threads_pool_;
    delete state;
}

void TaskSystemParallelThreadPoolSpinning::spin(){
    int id;
    int total;
    while (true)
    {
        if (killed)
        {
            break;
        }
        state->mutex_->lock();
        total=state->num_total_tasks_;
        id=total-state->left_tasks_;
        if(id<total)state->left_tasks_--;
        state->mutex_->unlock();
        if(id<total){
            state->runnable_->runTask(id,total);
            state->mutex_->lock();
            state->finished_tasks_++;
            if(state->finished_tasks_==total){
                state->mutex_->unlock();
                state->finished_mutex_->lock();
                state->finished_mutex_->unlock();
                state->finished_->notify_all();
            }else{
                state->mutex_->unlock();
            }
        }
    }
    
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    std::unique_lock<std::mutex>lk(*(state->finished_mutex_));
    state->mutex_->lock();
    state->finished_tasks_=0;
    state->left_tasks_=num_total_tasks;
    state->num_total_tasks_=num_total_tasks;
    state->runnable_=runnable;
    state->mutex_->unlock();
    state->finished_->wait(lk);
    lk.unlock();
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
