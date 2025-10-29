-- Tabla para eventos de dominio
CREATE TABLE IF NOT EXISTS DomainEventEntry (
    globalIndex BIGSERIAL NOT NULL,
    eventIdentifier VARCHAR(255) NOT NULL,
    timeStamp VARCHAR(255) NOT NULL,
    type VARCHAR(255) NOT NULL,
    aggregateIdentifier VARCHAR(255) NOT NULL,
    sequenceNumber BIGINT NOT NULL,
    metaData TEXT,
    payload TEXT NOT NULL,
    payloadRevision VARCHAR(255),
    payloadType VARCHAR(255) NOT NULL,
    PRIMARY KEY (globalIndex),
    UNIQUE (aggregateIdentifier, sequenceNumber),
    UNIQUE (eventIdentifier)
);

CREATE INDEX IF NOT EXISTS idx_domain_event_aggregate 
    ON DomainEventEntry (aggregateIdentifier, sequenceNumber);

CREATE INDEX IF NOT EXISTS idx_domain_event_timestamp 
    ON DomainEventEntry (timeStamp);

-- Tabla para snapshots
CREATE TABLE IF NOT EXISTS SnapshotEventEntry (
    aggregateIdentifier VARCHAR(255) NOT NULL,
    sequenceNumber BIGINT NOT NULL,
    type VARCHAR(255) NOT NULL,
    eventIdentifier VARCHAR(255) NOT NULL,
    timeStamp VARCHAR(255) NOT NULL,
    metaData TEXT,
    payload TEXT NOT NULL,
    payloadRevision VARCHAR(255),
    payloadType VARCHAR(255) NOT NULL,
    PRIMARY KEY (aggregateIdentifier, sequenceNumber),
    UNIQUE (eventIdentifier)
);

-- Tabla para tracking de tokens (event processors)
CREATE TABLE IF NOT EXISTS TokenEntry (
    processorName VARCHAR(255) NOT NULL,
    segment INTEGER NOT NULL,
    token BYTEA,
    tokenType VARCHAR(255),
    timestamp VARCHAR(255),
    owner VARCHAR(255),
    PRIMARY KEY (processorName, segment)
);

-- Tabla para tracking de sagas (opcional, por si usas sagas)
CREATE TABLE IF NOT EXISTS SagaEntry (
    sagaId VARCHAR(255) NOT NULL,
    revision VARCHAR(255),
    sagaType VARCHAR(255),
    serializedSaga BYTEA,
    PRIMARY KEY (sagaId)
);

CREATE TABLE IF NOT EXISTS AssociationValueEntry (
    id BIGSERIAL NOT NULL,
    associationKey VARCHAR(255) NOT NULL,
    associationValue VARCHAR(255),
    sagaId VARCHAR(255) NOT NULL,
    sagaType VARCHAR(255),
    PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS idx_saga_association 
    ON AssociationValueEntry (sagaId, sagaType);

CREATE INDEX IF NOT EXISTS idx_saga_association_value 
    ON AssociationValueEntry (associationKey, associationValue);