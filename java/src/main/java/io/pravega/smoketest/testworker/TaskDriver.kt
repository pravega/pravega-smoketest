package io.pravega.smoketest.testworker

import org.slf4j.LoggerFactory
import io.pravega.smoketest.testworker.workers.TaskPerformanceCollector
import java.util.function.Consumer

abstract class TaskDriver(private val messageClient: MessageClient): Runnable {

    var taskFinishedHandler = { _:Any -> }
        private set
    private var taskFailedHandler = { _:Throwable ->  }
    private var aborted = false
    private var started = false
    val performanceCollector = TaskPerformanceCollector(messageClient)

    companion object {
        private val LOG = LoggerFactory.getLogger(TaskDriver::class.java)
    }

    final override fun run() {
        try {
            prepare()

            if (!aborted) {
                start()
            } else {
                LOG.info("Test Aborted, skipping start")
            }
        }
        catch (e: Throwable) {
            LOG.error("TaskDriver Failed", e)
            taskFailedHandler(e)
        }
    }

    open fun onAbortMessage() {
        LOG.info("Got abort message")
        if (!started) {
            aborted = true
        }
        else {
            LOG.info("Task already started, ")
        }
    }

    fun onTaskFinished(handler: Consumer<Any>) {
        taskFinishedHandler = { finishedValue ->
            handler.accept(finishedValue)
        }
    }

    fun onTaskFailed(handler: Consumer<Throwable>) {
        taskFailedHandler = { finishedValue ->
            handler.accept(finishedValue)
        }
    }

    abstract protected fun prepare()
    abstract protected fun start()
    abstract fun kill()
}
