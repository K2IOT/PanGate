# ADR 0008: Monorepo tạm thời, giữ build độc lập cho từng thư mục (polyrepo-ready)

## Trạng thái

Accepted, 2026-11-05.

## Bối cảnh

Base project (`platform-parent`, `platform-contracts`, `platform-spring-starter`, `gateway-devtools`, `platform-devenv`) và gateway phát triển cùng lúc; về sau có thể tách repo.

## Quyết định

- Một repo, mỗi thư mục một `pom.xml`, không có pom aggregator; module phụ thuộc nhau qua artifact đã `mvn install` (sau này qua GitHub Packages).
- `platform-parent` pin chính xác version mọi dependency ngoài; enforcer bật.
- `Makefile` ở gốc build theo thứ tự parent → contracts → starter → devtools → gateway.

## Hệ quả

- Tách repo sau này không phải sửa build.
- Phải `make bootstrap` sau khi đổi module nền; không có build một lệnh kiểu reactor.

## Nguồn

Spec §2.
