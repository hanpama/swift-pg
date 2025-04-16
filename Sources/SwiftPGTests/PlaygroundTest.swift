import Foundation
import NIO
import Testing

@testable import SwiftPG

final class PlaygroundTest {
  @Test func testPlaygroundTest() async throws {
    // let loopGroup1 = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    // let conn1 = try await PostgreSQLConnection(configs: getSecureConfigs())

    // let loopGroup2 = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    // let conn2 = try await PostgreSQLConnection(configs: getSecureConfigs())

    // print(loopGroup1.description)
    // try await loopGroup1.next().submit {
    //   print("Hey!")
    // }.get()

    // for try await row in try await conn2.query("SELECT 1;") {
    //   let value = try row.decode(Int.self)
    //   #expect(value == 1)
    //   print(value)
    // }

    // let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)

    // let promise = eventLoopGroup.next().makePromise(of: Int.self)
    // promise.fail(NSError(domain: "Test", code: 0, userInfo: nil))

    // promise.succeed(3)
    // promise.succeed(3)

    // let task = Task {
    //   try await Task.sleep(nanoseconds: 1_000_000_000)
    //   return 3
    // }
    // task.cancel()

    // print([1,2,3,4,5][0..<2])

    // print(["1", ""].joined(separator: "."))

    // print("\(Decimal(string: "0.")!)")
    // print("\(Decimal(string: "1.0")!)")
    // print("\(Decimal(string: "0.4")!)")

    // let p = "12345678912345"

    // let splitArray = stride(from: 0, to: p.count, by: 4).map { i -> String in
    //     let end = p.index(p.endIndex, offsetBy: -i)
    //     let start = p.index(end, offsetBy: -4, limitedBy: p.startIndex) ?? p.startIndex
    //     return String(p[start..<end])
    // }.reversed()

    // print(Array(splitArray))  // [\"12\", \"3456\", \"7891\", \"2345\"]

  }
}
