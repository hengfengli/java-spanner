package com.google.cloud.spanner;

import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.*;
import com.google.api.gax.rpc.AbortedException;
import com.google.cloud.spanner.spi.v1.SpannerRpc;
import com.google.common.base.Stopwatch;
import com.google.common.base.Ticker;
import com.google.protobuf.ByteString;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.PartialResultSet;
import io.grpc.Status;
import io.opencensus.trace.Span;
import org.threeten.bp.Duration;
import org.threeten.bp.temporal.ChronoUnit;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.google.common.base.Preconditions.checkState;

public class PartitionedDmlTransaction2 implements SessionImpl.SessionTransaction {
  private static final Logger LOGGER = Logger.getLogger(PartitionedDmlTransaction2.class.getName());

  private final SessionImpl session;
  private final SpannerRpc rpc;
  private final Ticker ticker;
  private final PartitionedDmlTransactionRequestFactory requestFactory;
  private volatile boolean isValid = true;

  PartitionedDmlTransaction2(SessionImpl session,
                             SpannerRpc rpc,
                             Ticker ticker,
                             PartitionedDmlTransactionRequestFactory requestFactory) {
    this.session = session;
    this.rpc = rpc;
    this.ticker = ticker;
    this.requestFactory = requestFactory;
  }

  /**
   * Executes the {@link Statement} using a partitioned dml transaction with automatic retry if the
   * transaction was aborted. The update method uses the ExecuteStreamingSql RPC to execute the
   * statement, and will retry the stream if an {@link UnavailableException} is thrown, using the
   * last seen resume token if the server returns any.
   */
  long executeStreamingPartitionedUpdate(final Statement statement,
                                         final Duration timeout) {
    checkState(isValid, "Partitioned DML has been invalidated by a new operation on the session");
    LOGGER.log(Level.FINER, "Starting PartitionedUpdate statement");

    ByteString resumeToken = ByteString.EMPTY;
    boolean foundStats = false;
    long updateCount = 0L;
    Stopwatch stopwatch = Stopwatch.createStarted(ticker);

    try {
      ExecuteSqlRequest request = requestFactory.newTransactionRequestFrom(statement);

      while (true) {
        final Duration remainingTimeout = tryUpdateTimeout(timeout, stopwatch);

        try {
          ServerStream<PartialResultSet> stream = rpc.executeStreamingPartitionedDml(
              request,
              session.getOptions(),
              remainingTimeout
          );

          for (PartialResultSet rs : stream) {
            if (rs.getResumeToken() != null && !rs.getResumeToken().isEmpty()) {
              resumeToken = rs.getResumeToken();
            }
            if (rs.hasStats()) {
              foundStats = true;
              updateCount += rs.getStats().getRowCountLowerBound();
            }
          }
          break;
        } catch (UnavailableException e) {
          LOGGER.log(Level.FINER, "Retrying PartitionedDml transaction after UnavailableException", e);
          request = resumeOrRestartRequest(resumeToken, statement, request);
        } catch (AbortedException e) {
          LOGGER.log(Level.FINER, "Retrying PartitionedDml transaction after AbortedException", e);
          resumeToken = ByteString.EMPTY;
          foundStats = false;
          updateCount = 0L;
          request = requestFactory.newTransactionRequestFrom(statement);
        } catch (InternalException e) {
          if (isEosExceptione(e)) {
            LOGGER.log(Level.FINER, "Retrying PartitionedDml transaction after InternalException - EOS", e);
            request = resumeOrRestartRequest(resumeToken, statement, request);
          } else {
            throw e;
          }
        }
      }
      if (!foundStats) {
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.INVALID_ARGUMENT,
            "Partitioned DML response missing stats possibly due to non-DML statement as input"
        );
      }
      LOGGER.log(Level.FINER, "Finished PartitionedUpdate statement");
      return updateCount;
    } catch (Exception e) {
      throw SpannerExceptionFactory.newSpannerException(e);
    }
  }

  private boolean isEosExceptione(InternalException e) {
    return e.getMessage().contains("Received unexpected EOS on DATA frame from server");
  }

  @Override
  public void invalidate() {
    isValid = false;
  }

  // No-op method needed to implement SessionTransaction interface.
  @Override
  public void setSpan(Span span) {
  }

  private Duration tryUpdateTimeout(final Duration timeout,
                                    final Stopwatch stopwatch) {
    final Duration remainingTimeout = timeout.minus(stopwatch.elapsed(TimeUnit.MILLISECONDS), ChronoUnit.MILLIS);
    if (remainingTimeout.isNegative() || remainingTimeout.isZero()) {
      // The total deadline has been exceeded while retrying.
      throw new DeadlineExceededException(
          null, GrpcStatusCode.of(Status.Code.DEADLINE_EXCEEDED), false
      );
    }
    return remainingTimeout;
  }

  private ExecuteSqlRequest resumeOrRestartRequest(final ByteString resumeToken,
                                                   final Statement statement,
                                                   final ExecuteSqlRequest originalRequest) {
    if (resumeToken.isEmpty()) {
      return requestFactory.newTransactionRequestFrom(statement);
    } else {
      return ExecuteSqlRequest.newBuilder(originalRequest).setResumeToken(resumeToken).build();
    }
  }
}
