package com.google.cloud.spanner;

import com.google.cloud.spanner.spi.v1.SpannerRpc;
import com.google.protobuf.ByteString;
import com.google.spanner.v1.*;

import java.util.Map;

public class PartitionedDmlTransactionRequestFactory {

  private final SessionImpl session;
  private final SpannerRpc rpc;

  public PartitionedDmlTransactionRequestFactory(SessionImpl session,
                                                 SpannerRpc rpc) {
    this.session = session;
    this.rpc = rpc;
  }

  ExecuteSqlRequest newTransactionRequestFrom(final Statement statement) {
    ByteString transactionId = initTransaction();

    final TransactionSelector transactionSelector = TransactionSelector
        .newBuilder()
        .setId(transactionId)
        .build();
    final ExecuteSqlRequest.Builder builder = ExecuteSqlRequest
        .newBuilder()
        .setSql(statement.getSql())
        .setQueryMode(ExecuteSqlRequest.QueryMode.NORMAL)
        .setSession(session.getName())
        .setTransaction(transactionSelector);

    setParameters(builder, statement.getParameters());

    builder.setResumeToken(ByteString.EMPTY);

    return builder.build();
  }

  private ByteString initTransaction() {
    final BeginTransactionRequest request = BeginTransactionRequest
        .newBuilder()
        .setSession(session.getName())
        .setOptions(TransactionOptions
            .newBuilder()
            .setPartitionedDml(TransactionOptions.PartitionedDml.getDefaultInstance())
        ).build();
    Transaction tx = rpc.beginTransaction(request, session.getOptions());
    if (tx.getId().isEmpty()) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.INTERNAL,
          "Failed to init transaction, missing transaction id\n" + session.getName()
      );
    }
    return tx.getId();
  }

  private void setParameters(final ExecuteSqlRequest.Builder requestBuilder,
                             final Map<String, Value> statementParameters) {
    if (!statementParameters.isEmpty()) {
      com.google.protobuf.Struct.Builder paramsBuilder = requestBuilder.getParamsBuilder();
      for (Map.Entry<String, Value> param : statementParameters.entrySet()) {
        paramsBuilder.putFields(param.getKey(), param.getValue().toProto());
        requestBuilder.putParamTypes(param.getKey(), param.getValue().getType().toProto());
      }
    }
  }
}
