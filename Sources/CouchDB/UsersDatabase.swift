/**
 * Copyright IBM Corporation 2016, 2017
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

import Foundation
import KituraNet

// MARK: Users Database

/// Represents a CouchDB database of users.
public class UsersDatabase: Database {

    public struct User: Codable {
        public var name: String
        public var password: String
        public var type: String = "user"
        public var roles: [String] = []
        
        public init(name: String, password: String) {
            self.name = name
            self.password = password
        }
    }

    /// Create new user by name and password.
    ///
    /// - parameters:
    ///     - name: Username String.
    ///     - password: Password String.
    ///     - callback: Callback containing the username, JSON response,
    ///                 and an NSError if one occurred.
    public func createUser(_ document: User, callback: @escaping (String?, User?, NSError?) -> ()) {
        let id = "org.couchdb.user:\(document.name)"
        var doc: User?
        let requestOptions = CouchDBUtils.prepareRequest(connProperties,
                                                         method: "PUT",
                                                         path: "/_users/\(id)",
            hasBody: true,
            contentType: "application/json")
        let req = HTTP.request(requestOptions) { response in
            var error: NSError?
            if let response = response {
                do {
                    if response.statusCode != HTTPStatusCode.created && response.statusCode != HTTPStatusCode.accepted {
                        error = CouchDBUtils.createError(response.statusCode, errorDesc: try CouchDBUtils.getBodyObject(response), id: id, rev: nil)
                    } else {
                        doc = try CouchDBUtils.getBodyObject(response)
                    }
                } catch let caughtError {
                    #if os(Linux)
                    error = NSError(domain: caughtError.localizedDescription, code: response.statusCode)
                    #else
                    error = caughtError as NSError
                    #endif
                }
            } else {
                error = CouchDBUtils.createError(Database.InternalError, id: id, rev: nil)
            }
            callback(id, doc, error)
        }
        do {
            req.end(try JSONEncoder().encode(document))
        } catch _ {
            req.end()
        }
    }

    /// Get a user by name.
    ///
    /// - parameters:
    ///     - name: Name String of the desired user.
    ///     - callback: Callback containing the user JSON, or an NSError if one occurred.
    public func getUser(name: String, callback: @escaping (User?, NSError?) -> ()) {
        let id = "org.couchdb.user:\(name)"
        retrieve(id, callback: { (doc: User?, error) in
            #if os(Linux)
                callback(doc, error)
            #else
                callback(doc, error)
            #endif
        })
    }
}
