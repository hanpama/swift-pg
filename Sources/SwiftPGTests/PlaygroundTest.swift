import Foundation
import NIO
import Testing

final class PlaygroundTest {
  @Test func test() async throws {
    let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)

    let promise = eventLoopGroup.next().makePromise(of: Int.self)
    promise.fail(NSError(domain: "Test", code: 0, userInfo: nil))

    promise.succeed(3)
    promise.succeed(3)

    print(try await promise.futureResult.get())

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
