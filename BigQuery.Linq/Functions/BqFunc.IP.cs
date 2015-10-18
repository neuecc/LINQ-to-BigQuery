using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq
{
    public static partial class BqFunc
    {
        // IP functions convert IP addresses to and from human-readable form.

        /// <summary>
        /// Converts 32 least significant bits of integer_value to human-readable IPv4 address string.
        /// For example, FORMAT_IP(1) will return string '0.0.0.1'.
        /// </summary>
        [FunctionName("FORMAT_IP")]
        public static string FormatIP(int value) { throw Invalid(); }

        /// <summary>
        /// Converts a string representing IPv4 address to unsigned integer value.
        /// For example, PARSE_IP('0.0.0.1') will return 1. If string is not a valid IPv4 address, PARSE_IP will return NULL.
        /// </summary>
        [FunctionName("PARSE_IP")]
        public static long ParseIP(string readableIP) { throw Invalid(); }


        // IPAddress supports writing IPv4 and IPv6 addresses in packed strings, as 4- or 16-byte binary data in network byte order.
        // The functions described below supports parsing the addresses to and from human readable form.
        // These functions work only on string fields with IPs.


        /// <summary>
        /// Returns a human-readable IP address, in the form 10.1.5.23 or 2620:0:1009:1:216:36ff:feef:3f.
        /// Examples:
        /// FORMAT_PACKED_IP('0123456789@ABCDE') returns '3031:3233:3435:3637:3839:4041:4243:4445'
        /// FORMAT_PACKED_IP('0123') returns '48.49.50.51'
        /// </summary>
        [FunctionName("FORMAT_PACKED_IP")]
        public static string FormatPackedIP(string packedIP) { throw Invalid(); }

        /// <summary>
        /// Returns an IP address in binary format packed in a string.
        /// If the input string is not a valid IPv4 or IPv6 address, PARSE_PACKED_IP will return NULL.
        /// Examples:
        /// PARSE_PACKED_IP('48.49.50.51') returns '0123'
        /// PARSE_PACKED_IP('3031:3233:3435:3637:3839:4041:4243:4445') returns '0123456789@ABCDE'
        /// </summary>
        [FunctionName("PARSE_PACKED_IP")]
        public static string ParsePackedIP(string readableIP) { throw Invalid(); }
    }
}
