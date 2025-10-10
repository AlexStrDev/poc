package com.example.banking.event;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@AllArgsConstructor
@NoArgsConstructor
public class AccountCreatedEvent {
    private String accountId;
    private String owner;
    private double initialBalance;
}