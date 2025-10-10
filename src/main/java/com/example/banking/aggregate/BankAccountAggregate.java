package com.example.banking.aggregate;

import com.example.banking.command.CreateAccountCommand;
import com.example.banking.command.DepositMoneyCommand;
import com.example.banking.command.WithdrawMoneyCommand;
import com.example.banking.event.AccountCreatedEvent;
import com.example.banking.event.MoneyDepositedEvent;
import com.example.banking.event.MoneyWithdrawnEvent;

import org.axonframework.commandhandling.CommandHandler;
import org.axonframework.eventsourcing.EventSourcingHandler;
import org.axonframework.modelling.command.AggregateCreationPolicy;
import org.axonframework.modelling.command.AggregateIdentifier;
import org.axonframework.modelling.command.AggregateLifecycle;
import org.axonframework.modelling.command.CreationPolicy;
import org.axonframework.spring.stereotype.Aggregate;

@Aggregate
public class BankAccountAggregate {

    @AggregateIdentifier
    private String accountId;
    private String owner;
    private double balance;

    public BankAccountAggregate() {
    }
    
    @CommandHandler
    @CreationPolicy(AggregateCreationPolicy.ALWAYS)
    public AccountCreatedEvent handle(CreateAccountCommand command) {
        if (command.getInitialBalance() < 0) {
            throw new IllegalArgumentException("El balance inicial no puede ser negativo");
        }
        System.out.println("Creando cuenta para::::::::: " );

        AccountCreatedEvent event = new AccountCreatedEvent(
            command.getAccountId(),
            command.getOwner(),
            command.getInitialBalance()
        );

        AggregateLifecycle.apply(event);
        return event;
    }

    @CommandHandler
    public void handle(DepositMoneyCommand command) {
        if (command.getAmount() <= 0) {
            throw new IllegalArgumentException("El monto a depositar debe ser mayor a cero");
        }
        
        AggregateLifecycle.apply(new MoneyDepositedEvent(
            command.getAccountId(),
            command.getAmount()
        ));
    }

    @CommandHandler
    public void handle(WithdrawMoneyCommand command) {
        if (command.getAmount() <= 0) {
            throw new IllegalArgumentException("El monto a retirar debe ser mayor a cero");
        }
        
        if (this.balance < command.getAmount()) {
            throw new IllegalArgumentException("Fondos insuficientes. Balance actual: " + this.balance);
        }
        
        AggregateLifecycle.apply(new MoneyWithdrawnEvent(
            command.getAccountId(),
            command.getAmount()
        ));
    }

    @EventSourcingHandler
    public void on(AccountCreatedEvent event) {
        this.accountId = event.getAccountId();
        this.owner = event.getOwner();
        this.balance = event.getInitialBalance();
        System.out.println("Cuenta creada con ID: " + this.accountId);
    }

    @EventSourcingHandler
    public void on(MoneyDepositedEvent event) {
        this.balance += event.getAmount();
    }

    @EventSourcingHandler
    public void on(MoneyWithdrawnEvent event) {
        this.balance -= event.getAmount();
    }
}