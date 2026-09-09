// swift-tools-version:5.0

import PackageDescription

let package = Package(
  name: "ZeeQL3PCK",

  products: [ // TBD: Use ZeeQL3 as library name?
    .library(name: "PostgreSQLAdaptor", targets: [ "PostgreSQLAdaptor" ])
  ],
  dependencies: [
    .package(url: "https://github.com/codewinsdotcom/PostgresClientKit", 
             from: "1.3.1"),
    .package(url: "https://github.com/ZeeQL/ZeeQL3.git", from: "0.10.0")
  ],
  targets: [
    .target(name: "PostgreSQLAdaptor",
            dependencies: [ "PostgresClientKit", "ZeeQL" ]),
    .testTarget(name: "PostgreSQLAdaptorTests",
                dependencies: [ "PostgreSQLAdaptor" ])
  ]
)
