package com.example.banking.query.projection;

import com.example.banking.event.AccountCreatedEvent;
import com.example.banking.event.MoneyDepositedEvent;
import com.example.banking.event.MoneyWithdrawnEvent;
import com.example.banking.query.entity.BankAccount;
import com.example.banking.query.repository.BankAccountRepository;
import org.axonframework.eventhandling.EventHandler;
import org.springframework.stereotype.Component;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Optional;

@Component
@RequiredArgsConstructor
public class BankAccountProjection {

    private final BankAccountRepository repository;

    // Maneja el evento de cuenta creada
    @EventHandler
    public void on(AccountCreatedEvent event) {
        BankAccount account = new BankAccount(
            event.getAccountId(),
            event.getOwner(),
            event.getInitialBalance()
        );
        System.out.println("Account created (llego al projection): " + account);
        // repository.save(account);
    }

    // Maneja el evento de dinero depositado
    @EventHandler
    public void on(MoneyDepositedEvent event) {
        Optional<BankAccount> accountOpt = repository.findById(event.getAccountId());
        if (accountOpt.isPresent()) {
            BankAccount account = accountOpt.get();
            account.setBalance(account.getBalance() + event.getAmount());
            // repository.save(account);
        }
    }

    // Maneja el evento de dinero retirado
    @EventHandler
    public void on(MoneyWithdrawnEvent event) {
        Optional<BankAccount> accountOpt = repository.findById(event.getAccountId());
        if (accountOpt.isPresent()) {
            BankAccount account = accountOpt.get();
            account.setBalance(account.getBalance() - event.getAmount());
            // repository.save(account);
        }
    }

    // Método para consultar todas las cuentas
    public List<BankAccount> findAll() {
        return repository.findAll();
    }

    // Método para consultar una cuenta específica
    public Optional<BankAccount> findById(String accountId) {
        return repository.findById(accountId);
    }
}