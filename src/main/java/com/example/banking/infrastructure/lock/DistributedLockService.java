package com.example.banking.infrastructure.lock;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Servicio de locks distribuidos para prevenir condiciones de carrera.
 * 
 * Uso principal:
 * - Materialización concurrente de aggregates desde Kafka
 * - Prevenir que múltiples instancias materialicen el mismo aggregate simultáneamente
 * 
 * NOTA: Esta es una implementación in-memory para una sola instancia.
 * En producción con múltiples instancias, usar:
 * - Redis: RedisLockRegistry (Spring Integration)
 * - Hazelcast: HazelcastInstance.getLock()
 * - Zookeeper: InterProcessMutex (Curator)
 */
@Slf4j
@Service
public class DistributedLockService {

    private final Map<String, Lock> locks;

    public DistributedLockService() {
        this.locks = new ConcurrentHashMap<>();
    }

    /**
     * Intenta adquirir un lock con timeout.
     * 
     * @param key Identificador del recurso a bloquear (ej: "materialize:aggregate-123")
     * @param timeout Tiempo máximo de espera
     * @param unit Unidad de tiempo
     * @return true si se adquirió el lock, false si timeout
     */
    public boolean tryLock(String key, long timeout, TimeUnit unit) {
        Lock lock = locks.computeIfAbsent(key, k -> new ReentrantLock());
        
        try {
            boolean acquired = lock.tryLock(timeout, unit);
            
            if (acquired) {
                log.debug("Lock adquirido: {}", key);
            } else {
                log.warn("Timeout esperando lock: {}", key);
            }
            
            return acquired;
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupción esperando lock: {}", key, e);
            return false;
        }
    }

    /**
     * Libera un lock adquirido previamente.
     * 
     * @param key Identificador del recurso
     */
    public void unlock(String key) {
        Lock lock = locks.get(key);
        
        if (lock != null) {
            try {
                lock.unlock();
                log.debug("Lock liberado: {}", key);
            } catch (IllegalMonitorStateException e) {
                log.warn("Intento de liberar lock no adquirido: {}", key);
            }
        }
    }

    /**
     * Ejecuta una operación con lock automático.
     * 
     * @param key Identificador del recurso
     * @param timeout Tiempo máximo de espera
     * @param unit Unidad de tiempo
     * @param operation Operación a ejecutar
     * @return true si se ejecutó, false si no se pudo adquirir el lock
     */
    public boolean executeWithLock(String key, long timeout, TimeUnit unit, Runnable operation) {
        if (tryLock(key, timeout, unit)) {
            try {
                operation.run();
                return true;
            } finally {
                unlock(key);
            }
        }
        return false;
    }

    /**
     * Ejecuta una operación con lock y retorna resultado.
     */
    public <T> T executeWithLock(String key, long timeout, TimeUnit unit, 
                                  java.util.function.Supplier<T> operation) {
        if (tryLock(key, timeout, unit)) {
            try {
                return operation.get();
            } finally {
                unlock(key);
            }
        }
        throw new LockAcquisitionException("No se pudo adquirir lock: " + key);
    }

    /**
     * Obtiene estadísticas de locks
     */
    public Map<String, Object> getStats() {
        return Map.of(
            "activeLocks", locks.size()
        );
    }

    /**
     * Limpia locks no utilizados (para testing)
     */
    public void cleanup() {
        locks.clear();
    }

    /**
     * Excepción cuando no se puede adquirir un lock
     */
    public static class LockAcquisitionException extends RuntimeException {
        public LockAcquisitionException(String message) {
            super(message);
        }
    }
}