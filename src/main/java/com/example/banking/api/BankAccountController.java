package com.example.banking.api;

import com.example.banking.command.CreateAccountCommand;
import com.example.banking.command.DepositMoneyCommand;
import com.example.banking.command.WithdrawMoneyCommand;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Controlador REST para operaciones bancarias
 */
@Slf4j
@RestController
@RequestMapping("/api/accounts")
public class BankAccountController {

    private final CommandGateway commandGateway;

    public BankAccountController(CommandGateway commandGateway) {
        this.commandGateway = commandGateway;
    }

    @PostMapping
    public CompletableFuture<ResponseEntity<String>> createAccount(@RequestBody CreateAccountRequest request) {
        log.info("Creando cuenta para: {}", request.getOwner());
        
        String accountId = UUID.randomUUID().toString();
        CreateAccountCommand command = new CreateAccountCommand(
                accountId,
                request.getOwner(),
                request.getInitialBalance()
        );
        
        return commandGateway.send(command)
                .thenApply(result -> {
                    log.info("Cuenta creada exitosamente: {}", accountId);
                    return ResponseEntity.ok(accountId);
                })
                .exceptionally(throwable -> {
                    log.error("Error creando cuenta", throwable);
                    return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                            .body("Error: " + throwable.getMessage());
                });
    }

    @PostMapping("/{accountId}/deposit")
    public CompletableFuture<ResponseEntity<String>> deposit(
            @PathVariable("accountId") String accountId,
            @RequestBody TransactionRequest request) {
        
        log.info("Depositando {} en cuenta {}", request.getAmount(), accountId);
        
        DepositMoneyCommand command = new DepositMoneyCommand(accountId, request.getAmount());
        
        return commandGateway.send(command)
                .thenApply(result -> {
                    log.info("Depósito exitoso en cuenta: {}", accountId);
                    return ResponseEntity.ok("Depósito exitoso");
                })
                .exceptionally(throwable -> {
                    log.error("Error en depósito", throwable);
                    return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                            .body("Error: " + throwable.getMessage());
                });
    }

    @PostMapping("/{accountId}/withdraw")
    public CompletableFuture<ResponseEntity<String>> withdraw(
            @PathVariable("accountId") String accountId,
            @RequestBody TransactionRequest request) {
        
        log.info("Retirando {} de cuenta {}", request.getAmount(), accountId);
        
        WithdrawMoneyCommand command = new WithdrawMoneyCommand(accountId, request.getAmount());
        
        return commandGateway.send(command)
                .thenApply(result -> {
                    log.info("Retiro exitoso de cuenta: {}", accountId);
                    return ResponseEntity.ok("Retiro exitoso");
                })
                .exceptionally(throwable -> {
                    log.error("Error en retiro", throwable);
                    return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                            .body("Error: " + throwable.getMessage());
                });
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CreateAccountRequest {
        private String owner;
        private double initialBalance;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TransactionRequest {
        private double amount;
    }
}