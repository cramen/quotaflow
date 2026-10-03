import Foundation

// Public macOS API; pmset's legacy thermal query is unavailable on some Apple Silicon hosts.
let process = ProcessInfo.processInfo
let state = ["thermalState": process.thermalState.rawValue,
             "lowPowerMode": process.isLowPowerModeEnabled ? 1 : 0]
let data = try JSONSerialization.data(withJSONObject: state, options: [.sortedKeys])
print(String(data: data, encoding: .utf8)!)
