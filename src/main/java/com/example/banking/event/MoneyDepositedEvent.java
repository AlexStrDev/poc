package com.example.banking.event;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@AllArgsConstructor
@NoArgsConstructor
public class MoneyDepositedEvent {
    private String accountId;
    private double amount;
}
