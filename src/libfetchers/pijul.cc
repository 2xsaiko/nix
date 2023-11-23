#include "fetchers.hh"
#include "store-api.hh"

using namespace std::string_literals;
using namespace std::string_view_literals;

namespace nix::fetchers {

struct PijulInputScheme : InputScheme
{
    [[nodiscard]]
    std::optional<Input> inputFromURL(const ParsedURL &url) const override
    {
        if (url.scheme != "pijul+http" && url.scheme != "pijul+https") {
            return {};
        }

        auto url2(url);
        url2.scheme = std::string(url2.scheme, 6);
        url2.query.clear();

        Attrs attrs;
        attrs.emplace("type"s, "pijul"s);
        attrs.emplace("url"s, url2.to_string());

        return inputFromAttrs(attrs);
    }

    [[nodiscard]]
    std::optional<Input> inputFromAttrs(const Attrs &attrs) const override
    {
        if (maybeGetStrAttr(attrs, "type") != "pijul") {
            return {};
        }

        for (const auto &[name, _]: attrs) {
            if (name != "type" && name != "url") {
                throw Error("unsupported Pijul input attribute '%s'", name);
            }
        }

        parseURL(getStrAttr(attrs, "url"));

        Input input;
        input.attrs = attrs;
        return input;
    }

    [[nodiscard]]
    bool hasAllInfo(const Input &input) const override
    {
        return true;
    }

    [[nodiscard]]
    ParsedURL toURL(const Input &input) const override
    {
        auto url = parseURL(getStrAttr(input.attrs, "url"));

        if (url.scheme != "pijul") {
            url.scheme = "pijul+"s + url.scheme;
        }

        return url;
    }

    std::pair<StorePath, Input> fetch(
        ref<Store> store,
        const Input &_input
    ) override
    {
        Input input(_input);

        const Path tmpDir = createTempDir();
        const AutoDelete delTmpDir(tmpDir, true);
        const auto repoDir = tmpDir + "/source"sv;

        const auto url = parseURL(getStrAttr(input.attrs, "url"));
        const auto &repoUrl = url.base;

        runProgram("pijul"s, true, { "clone"s, repoUrl, repoDir }, {}, true);
        deletePath(repoDir + "/.pijul"sv);

        auto storePath = store->addToStore(input.getName(), repoDir);

        return { std::move(storePath), input };
    }
};

static auto rPijulInputScheme = OnStartup([] {
    registerInputScheme(std::make_unique<PijulInputScheme>());
});

} // namespace nix::fetchers
