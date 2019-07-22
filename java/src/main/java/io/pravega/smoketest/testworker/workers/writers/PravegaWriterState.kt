package io.pravega.smoketest.testworker.workers.writers

import io.pravega.smoketest.testworker.workers.PravegaTestPayload
import io.pravega.smoketest.testworker.workers.TaskPerformanceCollector
import io.pravega.client.stream.EventStreamWriter
import io.pravega.client.stream.Transaction
import io.pravega.client.stream.TxnFailedException
import org.slf4j.LoggerFactory
import java.util.Random
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

class PravegaWriterState(val sharedExecutor: ScheduledExecutorService, val performanceCollector: TaskPerformanceCollector) {
    private var transactionSize: Int = 2
    private var willHaveTransactions: Boolean = false
    private val LOG = LoggerFactory.getLogger(PravegaWriterState::class.java)
    lateinit var producer : EventStreamWriter<PravegaTestPayload>
    private var pendingTransactionAck = CompletableFuture<Void>()

    enum class TransactionMode(val isTransaction: Boolean, val willBeSuccessful: Boolean) {
        REGULAR_WRITE(false, true),
        TRANSACTION(true, true),
        ABORTED_TRANSACTION(true, false)
    }

    var currentState: TransactionMode? = null
        private set

    private var currentTransaction: Transaction<PravegaTestPayload>? = null
    private var transactionRecordsWritten = 0
    private val rgen = Random()

    fun beforeWrite() {
        currentState = getTransactionState()

        if (currentState!!.isTransaction && currentTransaction == null) {
            try {
                currentTransaction = producer.beginTxn()
                pendingTransactionAck = CompletableFuture()
                forkWatchProcess(pendingTransactionAck, currentTransaction!!, currentState!!)
                performanceCollector.txStarted()
            } catch (e: Throwable) {
                if (e is InterruptedException) {
                    throw e
                } else {
                    LOG.error("Error with BeginTransaction", e)
                }
            }
        }
    }

    fun doWrite(routingKey: String, payload: PravegaTestPayload): CompletableFuture<Void>? {
        val willAbort = !currentState!!.willBeSuccessful
        payload.isFromAbortedTransaction = willAbort

        val inTransaction = currentState!!.isTransaction

        if (inTransaction) {
            transactionRecordsWritten++
            currentTransaction!!.writeEvent(routingKey, payload)
            return pendingTransactionAck
        }
        return producer.writeEvent(routingKey, payload)
    }

    @Throws(TxnFailedException::class)
    fun afterWrite() {
        val thisTransaction = currentTransaction
        if (thisTransaction != null) {
            if (transactionRecordsWritten == transactionSize) {
                try {
                    if (currentState!!.willBeSuccessful) {
                        thisTransaction.commit()
                        performanceCollector.txCommited()
                    } else {
                        thisTransaction.abort()
                        performanceCollector.txAborted()
                    }
                } catch (e: TxnFailedException) {
                    performanceCollector.txFailed()
                    LOG.error("TX Failed to commit", e)
                    throw e
                }

                currentTransaction = null
                transactionRecordsWritten = 0
                currentState = null
                // we don't need to clear pendingTransactionAck; it's reset next time we do a transaction
            }
        } else {
            currentState = null
        }
    }

    fun isInTransaction() : Boolean {
        return currentTransaction != null
    }

    private fun getTransactionState() : TransactionMode {
        if (currentState != null) {
            return currentState!!
        }
        if (!willHaveTransactions) {
            return TransactionMode.REGULAR_WRITE
        }

        // have the same # of writes for transactions, aborted transactions, and non-transaction writes
        // this means we have 'less' transactions than raw writes
        val nextState = rgen.nextInt(2 + 2 * transactionSize)
        if (nextState == 0) {
            return TransactionMode.TRANSACTION
        } else if (nextState == 1) {
            return TransactionMode.ABORTED_TRANSACTION
        }
        return TransactionMode.REGULAR_WRITE
    }

    /**
     * This is a self-contained stateful method.
     *
     * Given a snapshot of the current transaction's details, this will watch in the background for the
     * transaction to finish. Then it will make the ack.
     */
    private fun forkWatchProcess(toAckOn: CompletableFuture<Void>, transactionToWatch: Transaction<PravegaTestPayload>, expectedState: TransactionMode) {
        val delay = 100L
        val atUnit = TimeUnit.MILLISECONDS

        val cancelWatchProcess = CompletableFuture<Void>()

        // monitor transaction state
        val watchProcess = sharedExecutor.scheduleWithFixedDelay({
            val status = transactionToWatch.checkStatus()
            if (status == Transaction.Status.COMMITTED) {
                if (expectedState == TransactionMode.TRANSACTION) {
                    toAckOn.complete(null)
                    cancelWatchProcess.complete(null)
                } else {
                    toAckOn.completeExceptionally(IllegalStateException("Got a committed transaction, didn't expect that"))
                    cancelWatchProcess.complete(null)
                }
            } else if (status == Transaction.Status.ABORTED) {
                if (expectedState == TransactionMode.ABORTED_TRANSACTION) {
                    toAckOn.complete(null)
                    cancelWatchProcess.complete(null)
                } else {
                    toAckOn.completeExceptionally(IllegalStateException("Got an aborted transaction, didn't expect that"))
                    cancelWatchProcess.complete(null)
                }
            }
        }, 0L, delay, atUnit)

        //when we get a terminal transaction state, cancel the watching
        // we can't do this in the watcher because the lambda doesn't have access to `watchProcess`
        cancelWatchProcess.whenComplete { _, _ ->
            watchProcess.cancel(false)
        }
    }

    fun withTransaction(transactionSize: Int): PravegaWriterState {
        this.willHaveTransactions = true
        this.transactionSize = transactionSize
        return this
    }

}
