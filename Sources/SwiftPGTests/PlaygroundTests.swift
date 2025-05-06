import Foundation
import NIO
import Testing

@testable import SwiftPG

final class PlaygroundTests {
    @Test func testPlaygroundTest() async throws {
        let conn = Connection()

        try await conn.connect(configs: getPlainSaslConnectionConfigs())

        // let promise = loopGroup.next().makePromise(of: Int.self)
        let task = Task {
            try await conn.execute("SELECT pg_sleep(1);")
            // do {
            //     // try await Task.sleep(nanoseconds: 1_000_000_000)
            //     promise.succeed(0)
            // } catch {
            //     print("Error: \(error)")
            //     promise.fail(error)
            // }
        }
        print("Canceling task")
        task.cancel()

        await #expect(throws: CancellationError.self) {
            let _ = try await task.result.get()
        }

        print(conn)
    }

    @Test func testPlaygroundTest2() async throws {
        let conn = Connection()

        try await conn.connect(configs: getPlainSaslConnectionConfigs())

        // let promise = loopGroup.next().makePromise(of: Int.self)
        let task = Task {
            print("t1: \(Date().timeIntervalSinceReferenceDate)")
            let rows = try await conn.query("SELECT pg_sleep(1);")
            print("t2: \(Date().timeIntervalSinceReferenceDate)")
            print(rows)
            // do {
            //     // try await Task.sleep(nanoseconds: 1_000_000_000)
            //     promise.succeed(0)
            // } catch {
            //     print("Error: \(error)")
            //     promise.fail(error)
            // }
        }
        print("Canceling task")
        task.cancel()

        await #expect(throws: CancellationError.self) {
            let _ = try await task.result.get()
        }
    }

    @Test func testPC() async throws {
        let loopGroup = MultiThreadedEventLoopGroup.singleton

        let pc = try await ProtocolClient(
            eventLoop: loopGroup.next(), configs: getPlainSaslConnectionConfigs())

        let task = Task {
            try await pc.receive()
        }
        task.cancel()
        await #expect(throws: CancellationError.self) {
            try await task.result.get()
            // print("Value", try await task.value as Any)
        }
        try await pc.close()
    }

    // @Test func testTaskGroup() async throws {

    //     let taskGroup = try await withTaskGroup(of: Void.self) { group in

    //     }
    // }

    @Test func test2() async throws {
        let loopGroup = MultiThreadedEventLoopGroup.singleton
        let t2 = Task {
            try await Task.sleep(nanoseconds: 1_000_000_000)
            print("Task 2")
        }
        let task = Task {
            try await t2.result.get()
            print("Task 1")
        }
        t2.cancel()

        await #expect(throws: CancellationError.self) {
            let _ = try await task.result.get()
        }

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
