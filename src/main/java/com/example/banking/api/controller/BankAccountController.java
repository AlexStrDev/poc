package com.example.banking.api.controller;

import com.example.banking.api.dto.CreateAccountDTO;
import com.example.banking.api.dto.TransactionDTO;
import com.example.banking.api.dto.AccountResponseDTO;
import com.example.banking.command.CreateAccountCommand;
import com.example.banking.command.DepositMoneyCommand;
import com.example.banking.command.WithdrawMoneyCommand;
import com.example.banking.query.projection.BankAccountProjection;
import lombok.RequiredArgsConstructor;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/accounts")
@RequiredArgsConstructor
public class BankAccountController {

    private final CommandGateway commandGateway;
    private final BankAccountProjection projection;

    // Crear una nueva cuenta
    @PostMapping
    public CompletableFuture<ResponseEntity<String>> createAccount(@RequestBody CreateAccountDTO dto) {
        String accountId = UUID.randomUUID().toString();
        
        CreateAccountCommand command = new CreateAccountCommand(
            accountId,
            dto.getOwner(),
            dto.getInitialBalance()
        );
        
        return commandGateway.send(command)
            .thenApply(result -> ResponseEntity.status(HttpStatus.CREATED)
                .body("Cuenta creada con ID: " + accountId))
            .exceptionally(ex -> ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body("Error: " + ex.getMessage()));
    }

    // Depositar dinero - AGREGADO EL NOMBRE DEL PARÁMETRO
    @PostMapping("/{accountId}/deposit")
    public CompletableFuture<ResponseEntity<String>> deposit(
            @PathVariable("accountId") String accountId,
            @RequestBody TransactionDTO dto) {
        
        DepositMoneyCommand command = new DepositMoneyCommand(accountId, dto.getAmount());
        
        return commandGateway.send(command)
            .thenApply(result -> ResponseEntity.ok("Depósito exitoso de $" + dto.getAmount()))
            .exceptionally(ex -> ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body("Error: " + ex.getMessage()));
    }

    // Retirar dinero - AGREGADO EL NOMBRE DEL PARÁMETRO
    @PostMapping("/{accountId}/withdraw")
    public CompletableFuture<ResponseEntity<String>> withdraw(
            @PathVariable("accountId") String accountId,
            @RequestBody TransactionDTO dto) {
        
        WithdrawMoneyCommand command = new WithdrawMoneyCommand(accountId, dto.getAmount());
        
        return commandGateway.send(command)
            .thenApply(result -> ResponseEntity.ok("Retiro exitoso de $" + dto.getAmount()))
            .exceptionally(ex -> ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body("Error: " + ex.getMessage()));
    }

    // Consultar balance de una cuenta - AGREGADO EL NOMBRE DEL PARÁMETRO
    @GetMapping("/{accountId}")
    public ResponseEntity<AccountResponseDTO> getAccount(
            @PathVariable("accountId") String accountId) {
        return projection.findById(accountId)
            .map(account -> ResponseEntity.ok(new AccountResponseDTO(
                account.getAccountId(),
                account.getOwner(),
                account.getBalance()
            )))
            .orElse(ResponseEntity.notFound().build());
    }

    // Listar todas las cuentas
    @GetMapping
    public ResponseEntity<List<AccountResponseDTO>> getAllAccounts() {
        List<AccountResponseDTO> accounts = projection.findAll().stream()
            .map(account -> new AccountResponseDTO(
                account.getAccountId(),
                account.getOwner(),
                account.getBalance()
            ))
            .collect(Collectors.toList());
        
        return ResponseEntity.ok(accounts);
    }
}