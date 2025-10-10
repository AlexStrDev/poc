package com.example.banking.command;

import org.axonframework.modelling.command.TargetAggregateIdentifier;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@AllArgsConstructor
@NoArgsConstructor
public class DepositMoneyCommand {
    @TargetAggregateIdentifier
    private String accountId;
    private double amount;
}
