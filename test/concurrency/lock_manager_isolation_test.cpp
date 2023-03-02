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

TEST(LockManagerTest, RepeatableRead) {
  const int num = 5;

  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  // table_oid_t oid = 0;

  std::stringstream result;
  auto bustub = std::make_unique<bustub::BustubInstance>();
  auto writer = bustub::SimpleStreamWriter(result, true, " ");

  auto schema = "CREATE TABLE nft(id int, terrier int);";
  std::cerr << "x: create schema" << std::endl;
  bustub->ExecuteSql(schema, writer);
  fmt::print("{}", result.str());

  std::cerr << "x: initialize data" << std::endl;
  std::string query = "INSERT INTO nft VALUES ";
  for (size_t i = 0; i < num; i++) {
    query += fmt::format("({}, {})", i, 0);
    if (i != num - 1) {
      query += ", ";
    } else {
      query += ";";
    }
  }

  {
    std::stringstream ss;
    auto writer = bustub::SimpleStreamWriter(ss, true);
    auto txn = bustub->txn_manager_->Begin(nullptr, bustub::IsolationLevel::REPEATABLE_READ);
    bustub->ExecuteSqlTxn(query, writer, txn);
    CheckGrowing(txn);
    bustub->txn_manager_->Commit(txn);
    delete txn;
    if (ss.str() != fmt::format("{}\t\n", num)) {
      fmt::print("unexpected result \"{}\" when insert\n", ss.str());
      exit(1);
    }
  }

  {
    std::string query = "SELECT * FROM nft;";
    std::stringstream ss;
    auto writer = bustub::SimpleStreamWriter(ss, true);
    auto txn = bustub->txn_manager_->Begin(nullptr, bustub::IsolationLevel::REPEATABLE_READ);
    bustub->ExecuteSqlTxn(query, writer, txn);
    CheckGrowing(txn);
    bustub->txn_manager_->Commit(txn);
    delete txn;
    fmt::print("--- YOUR RESULT ---\n{}\n", ss.str());
  }

  std::thread t0([&]() {
    std::string query = "select * from nft where id = 0";
    std::stringstream ss;
    auto writer = bustub::SimpleStreamWriter(ss, true);
    auto txn = bustub->txn_manager_->Begin(nullptr, bustub::IsolationLevel::REPEATABLE_READ);
    fmt::print("txn thread t0 {}\n", query);
    bustub->ExecuteSqlTxn(query, writer, txn);
    fmt::print("thread t0 result\n{}\n", ss.str());
    std::string s1 = ss.str();

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ss.str("");

    fmt::print("txn thread t0 {}\n", query);
    bustub->ExecuteSqlTxn(query, writer, txn);
    fmt::print("txn thread t0 result\n{}\n", ss.str());
    std::string s2 = ss.str();

    EXPECT_EQ(s1, s2);
    CheckGrowing(txn);
    bustub->txn_manager_->Commit(txn);
    fmt::print("txn threadt0 commit\n");
    delete txn;
  });

  std::thread t1([&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    std::string query = "update nft set terrier = 1 where id = 0";
    std::stringstream ss;
    auto writer = bustub::SimpleStreamWriter(ss, true);
    auto txn = bustub->txn_manager_->Begin(nullptr, bustub::IsolationLevel::REPEATABLE_READ);

    fmt::print("txn thread t1 {}\n", query);
    bustub->ExecuteSqlTxn(query, writer, txn);
    fmt::print("txn thread t1 result\n{}\n", ss.str());

    CheckGrowing(txn);
    bustub->txn_manager_->Commit(txn);
    fmt::print("txn thread t1 commit\n");
    delete txn;
  });

  t0.join();
  t1.join();
}

TEST(LockManagerTest, Readcommited) {
  const int num = 5;

  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  // table_oid_t oid = 0;

  std::stringstream result;
  auto bustub = std::make_unique<bustub::BustubInstance>();
  auto writer = bustub::SimpleStreamWriter(result, true, " ");

  auto schema = "CREATE TABLE nft(id int, terrier int);";
  std::cerr << "x: create schema" << std::endl;
  bustub->ExecuteSql(schema, writer);
  fmt::print("{}", result.str());

  std::cerr << "x: initialize data" << std::endl;
  std::string query = "INSERT INTO nft VALUES ";
  for (size_t i = 0; i < num; i++) {
    query += fmt::format("({}, {})", i, 0);
    if (i != num - 1) {
      query += ", ";
    } else {
      query += ";";
    }
  }

  {
    std::stringstream ss;
    auto writer = bustub::SimpleStreamWriter(ss, true);
    auto txn = bustub->txn_manager_->Begin(nullptr, bustub::IsolationLevel::REPEATABLE_READ);
    bustub->ExecuteSqlTxn(query, writer, txn);
    CheckGrowing(txn);
    bustub->txn_manager_->Commit(txn);
    delete txn;
    if (ss.str() != fmt::format("{}\t\n", num)) {
      fmt::print("unexpected result \"{}\" when insert\n", ss.str());
      exit(1);
    }
  }

  {
    std::string query = "SELECT * FROM nft;";
    std::stringstream ss;
    auto writer = bustub::SimpleStreamWriter(ss, true);
    auto txn = bustub->txn_manager_->Begin(nullptr, bustub::IsolationLevel::REPEATABLE_READ);
    bustub->ExecuteSqlTxn(query, writer, txn);
    CheckGrowing(txn);
    bustub->txn_manager_->Commit(txn);
    delete txn;
    fmt::print("--- YOUR RESULT ---\n{}\n", ss.str());
  }

  std::thread t0([&]() {
    std::string query = "select * from nft";
    std::stringstream ss;
    auto writer = bustub::SimpleStreamWriter(ss, true);

    auto txn = bustub->txn_manager_->Begin(nullptr, bustub::IsolationLevel::READ_COMMITTED);
    fmt::print("thread t0 {}\n", query);
    bustub->ExecuteSqlTxn(query, writer, txn);
    fmt::print("thread t0 result 0 \n{}\n", ss.str());
    std::string s1 = ss.str();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    ss.str("");  // 清空流， clear是清空标志位
    fmt::print("txn {}\n", query);
    bustub->ExecuteSqlTxn(query, writer, txn);
    fmt::print("thread t0 result 1 \n{}\n", ss.str());
    std::string s2 = ss.str();

    EXPECT_NE(s1, s2);
    CheckGrowing(txn);
    bustub->txn_manager_->Commit(txn);
    fmt::print("txn thread t0 commit\n");
    delete txn;
  });

  std::thread t1([&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    std::string query = "update nft set terrier = 1 where id = 0";
    std::stringstream ss;
    auto writer = bustub::SimpleStreamWriter(ss, true);
    auto txn = bustub->txn_manager_->Begin(nullptr, bustub::IsolationLevel::REPEATABLE_READ);

    fmt::print("txn thread t1 {}\n", query);
    bustub->ExecuteSqlTxn(query, writer, txn);
    fmt::print("txn thread t1 result\n{}", ss.str());

    CheckGrowing(txn);
    bustub->txn_manager_->Commit(txn);
    fmt::print("txn thread t1 commit\n");
    delete txn;
  });

  t0.join();
  t1.join();
}

}  // namespace bustub

/*
x: create schema
Table created with id = 0
x: initialize data
--- YOUR RESULT ---
0       0
1       0
2       0
3       0
4       0

txn thread t0 select * from nft where id = 0
thread t0 result
0       0

txn thread t1 update nft set terrier = 1 where id = 0
txn thread t0 select * from nft where id = 0
txn thread t0 result
0       0

txn threadt0 commit
txn thread t1 result
1

txn thread t1 commit
[       OK ] LockManagerTest.RepeatableRead (201 ms)
[ RUN      ] LockManagerTest.Readcommited
x: create schema
Table created with id = 0
x: initialize data
--- YOUR RESULT ---
0       0
1       0
2       0
3       0
4       0

thread t0 select * from nft
thread t0 result 0
0       0
1       0
2       0
3       0
4       0

txn thread t1 update nft set terrier = 1 where id = 0
txn thread t1 result
1
txn thread t1 commit
txn select * from nft
thread t0 result 1
0       1
1       0
2       0
3       0

*/
