/**
 * lock_manager_test.cpp
 */

#include "concurrency/lock_manager.h"

#include <chrono>  // NOLINT
#include <random>
#include <thread>  // NOLINT

#include "common/bustub_instance.h"
#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "gtest/gtest.h"

namespace bustub {

/*
 * This test is only a sanity check. Please do not rely on this test
 * to check the correctness.
 */

// --- Helper functions ---
void CheckGrowing(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::GROWING); }

void CheckShrinking(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::SHRINKING); }

void CheckAborted(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::ABORTED); }

void CheckCommitted(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::COMMITTED); }

void CheckTxnRowLockSize(Transaction *txn, table_oid_t oid, size_t shared_size, size_t exclusive_size) {
  EXPECT_EQ((*(txn->GetSharedRowLockSet()))[oid].size(), shared_size);
  EXPECT_EQ((*(txn->GetExclusiveRowLockSet()))[oid].size(), exclusive_size);
}

int GetTxnTableLockSize(Transaction *txn, LockManager::LockMode lock_mode) {
  switch (lock_mode) {
    case LockManager::LockMode::SHARED:
      return txn->GetSharedTableLockSet()->size();
    case LockManager::LockMode::EXCLUSIVE:
      return txn->GetExclusiveTableLockSet()->size();
    case LockManager::LockMode::INTENTION_SHARED:
      return txn->GetIntentionSharedTableLockSet()->size();
    case LockManager::LockMode::INTENTION_EXCLUSIVE:
      return txn->GetIntentionExclusiveTableLockSet()->size();
    case LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE:
      return txn->GetSharedIntentionExclusiveTableLockSet()->size();
  }

  return -1;
}

void CheckTableLockSizes(Transaction *txn, size_t s_size, size_t x_size, size_t is_size, size_t ix_size,
                         size_t six_size) {
  EXPECT_EQ(s_size, txn->GetSharedTableLockSet()->size());
  EXPECT_EQ(x_size, txn->GetExclusiveTableLockSet()->size());
  EXPECT_EQ(is_size, txn->GetIntentionSharedTableLockSet()->size());
  EXPECT_EQ(ix_size, txn->GetIntentionExclusiveTableLockSet()->size());
  EXPECT_EQ(six_size, txn->GetSharedIntentionExclusiveTableLockSet()->size());
}

TEST(LockManagerTest, CompatibilityTest1) {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  table_oid_t oid = 0;
  auto *txn0 = txn_mgr.Begin();
  auto *txn1 = txn_mgr.Begin();
  auto *txn2 = txn_mgr.Begin();
  // [S] SIX IS
  // [SIX IS]
  std::thread t0([&]() {
    bool res;
    res = lock_mgr.LockTable(txn0, LockManager::LockMode::SHARED, oid);
    EXPECT_TRUE(res);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    CheckTableLockSizes(txn1, 0, 0, 0, 0, 0);
    CheckTableLockSizes(txn2, 0, 0, 0, 0, 0);
    lock_mgr.UnlockTable(txn0, oid);
    txn_mgr.Commit(txn0);
  });

  std::thread t1([&]() {
    bool res;
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    res = lock_mgr.LockTable(txn1, LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE, oid);
    EXPECT_TRUE(res);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    CheckTableLockSizes(txn2, 0, 0, 1, 0, 0);
    res = lock_mgr.UnlockTable(txn1, oid);
    txn_mgr.Commit(txn1);
  });

  std::thread t2([&]() {
    bool res;
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    res = lock_mgr.LockTable(txn2, LockManager::LockMode::INTENTION_SHARED, oid);
    EXPECT_TRUE(res);
    std::this_thread::sleep_for(std::chrono::milliseconds(40));
    res = lock_mgr.UnlockTable(txn2, oid);
    txn_mgr.Commit(txn2);
  });

  t0.join();
  t1.join();
  t2.join();
  delete txn0;
  delete txn1;
  delete txn2;
}

TEST(LockManagerTest, CompatibilityTest2) {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  table_oid_t oid = 0;
  auto *txn0 = txn_mgr.Begin();
  auto *txn1 = txn_mgr.Begin();
  auto *txn2 = txn_mgr.Begin();
  // [IS IX] SIX
  // [IS SIX]
  std::thread t0([&]() {
    bool res;
    res = lock_mgr.LockTable(txn0, LockManager::LockMode::INTENTION_SHARED, oid);
    EXPECT_TRUE(res);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    CheckTableLockSizes(txn0, 0, 0, 1, 0, 0);
    CheckTableLockSizes(txn1, 0, 0, 0, 1, 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    CheckTableLockSizes(txn0, 0, 0, 1, 0, 0);
    CheckTableLockSizes(txn1, 0, 0, 0, 0, 0);
    CheckTableLockSizes(txn2, 0, 0, 0, 0, 1);
    lock_mgr.UnlockTable(txn0, oid);
    txn_mgr.Commit(txn0);
  });

  std::thread t1([&]() {
    bool res;
    res = lock_mgr.LockTable(txn1, LockManager::LockMode::INTENTION_EXCLUSIVE, oid);
    EXPECT_TRUE(res);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    res = lock_mgr.UnlockTable(txn1, oid);
    txn_mgr.Commit(txn1);
  });

  std::thread t2([&]() {
    bool res;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    res = lock_mgr.LockTable(txn2, LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE, oid);
    EXPECT_TRUE(res);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    res = lock_mgr.UnlockTable(txn2, oid);
    txn_mgr.Commit(txn2);
  });

  t0.join();
  t1.join();
  t2.join();

  delete txn0;
  delete txn1;
  delete txn2;
}

TEST(LockManagerTest, CompatibilityTest3) {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  table_oid_t oid = 0;

  auto *txn0 = txn_mgr.Begin();
  auto *txn1 = txn_mgr.Begin();
  auto *txn2 = txn_mgr.Begin();
  // [SIX] SIX IS
  // [SIX] [IS]
  std::thread t0([&]() {
    bool res;
    res = lock_mgr.LockTable(txn0, LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE, oid);
    EXPECT_TRUE(res);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    CheckTableLockSizes(txn1, 0, 0, 0, 0, 0);
    CheckTableLockSizes(txn2, 0, 0, 0, 0, 0);
    lock_mgr.UnlockTable(txn0, oid);
    txn_mgr.Commit(txn0);
  });

  std::thread t1([&]() {
    bool res;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    res = lock_mgr.LockTable(txn1, LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE, oid);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    EXPECT_TRUE(res);
    CheckTableLockSizes(txn1, 0, 0, 0, 0, 1);
    CheckTableLockSizes(txn2, 0, 0, 1, 0, 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    res = lock_mgr.UnlockTable(txn1, oid);
    txn_mgr.Commit(txn1);
  });

  std::thread t2([&]() {
    bool res;
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    res = lock_mgr.LockTable(txn2, LockManager::LockMode::INTENTION_SHARED, oid);
    EXPECT_TRUE(res);
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    res = lock_mgr.UnlockTable(txn2, oid);
    txn_mgr.Commit(txn2);
  });

  t0.join();
  t1.join();
  t2.join();
  delete txn0;
  delete txn1;
  delete txn2;
}

}  // namespace bustub
