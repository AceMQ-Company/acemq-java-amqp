/*
 * Copyright 2026 AceMQ.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.acemq.amqp.patterns;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Hands back the JDBC connection the calling code is already using for its own work.
 *
 * <p>This exists because the outbox is worth nothing unless its insert commits with the business
 * write, and only the application knows where its transaction lives. Under Spring that is
 * {@code DataSourceUtils.getConnection(dataSource)}, which returns the connection bound to the
 * current transaction; elsewhere it is usually a thread local the framework maintains.
 *
 * <p>What it must never be is {@code dataSource::getConnection}. A fresh connection with
 * auto-commit on will insert the outbox row immediately and independently, so a later rollback of
 * the business work leaves a message queued for something that never happened — the exact fault
 * the pattern was adopted to prevent, now harder to notice because the code looks right.
 *
 * <p>The connection returned here is borrowed, not owned: {@link JdbcOutboxStore} will not close
 * it, because the transaction that owns it is not finished.
 */
@FunctionalInterface
public interface ConnectionSupplier {

    /**
     * @return the connection belonging to the caller's current transaction
     * @throws SQLException if no connection can be obtained
     */
    Connection get() throws SQLException;
}
