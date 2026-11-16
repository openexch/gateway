#ifndef GATEWAY_TESTS_HTTP_CLIENT_H
#define GATEWAY_TESTS_HTTP_CLIENT_H

#include <chrono>
#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include <boost/asio/connect.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>

// A blocking HTTP/1.1 client for the in-process listener tests: one request per connection, optional raw headers.

namespace gwtest {

struct HttpResult
{
    int status = 0;
    std::string body;
    std::map<std::string, std::string> headers; // lower-case names
    double ms = 0;
    std::string error;
};

inline HttpResult httpCall(std::uint16_t port, const std::string &method, const std::string &target,
    const std::string &body = "", const std::vector<std::pair<std::string, std::string>> &headers = {},
    const std::string &contentType = "application/json")
{
    namespace beast = boost::beast;
    namespace http = beast::http;
    namespace net = boost::asio;
    HttpResult out;
    const auto start = std::chrono::steady_clock::now();
    try
    {
        net::io_context ioc;
        beast::tcp_stream stream(ioc);
        stream.expires_after(std::chrono::seconds(30));
        stream.connect(net::ip::tcp::endpoint(net::ip::make_address("127.0.0.1"), port));
        http::request<http::string_body> req(http::string_to_verb(method), target, 11);
        req.set(http::field::host, "127.0.0.1");
        if (!body.empty() && !contentType.empty())
        {
            req.set(http::field::content_type, contentType);
        }
        for (const auto &[k, v] : headers)
        {
            req.insert(k, v);
        }
        req.body() = body;
        req.prepare_payload();
        http::write(stream, req);
        beast::flat_buffer buffer;
        http::response_parser<http::string_body> parser;
        parser.body_limit(16 * 1024 * 1024);
        http::read(stream, buffer, parser);
        const auto &res = parser.get();
        out.status = static_cast<int>(res.result_int());
        out.body = res.body();
        for (const auto &f : res)
        {
            std::string name(f.name_string());
            for (char &c : name)
            {
                c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
            }
            out.headers[name] = std::string(f.value());
        }
        beast::error_code ec;
        stream.socket().shutdown(net::ip::tcp::socket::shutdown_both, ec);
    }
    catch (const std::exception &e)
    {
        out.error = e.what();
    }
    out.ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start).count();
    return out;
}

}

#endif
