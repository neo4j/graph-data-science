window.docMeta = (async function () {
  const version = '1.6';
  const name = 'graph-data-science';

  const href = window.location.href;
  const thisPubBaseUri = href.substring(0, href.indexOf(name) + name.length) + '/' + version;
  const unversionedDocBaseUri = href.substring(0, href.indexOf(name) + name.length) + '/';
  const commonDocsBaseUri = href.substring(0, href.indexOf(name) - 1);

  const pathname = window.location.pathname;
  let neo4jPageId;
  if (pathname.indexOf(name) > -1) {
    const baseUri = unversionedDocBaseUri + pathname.split(name + '/')[1].split('/')[0] + '/';
    neo4jPageId = href.replace(baseUri, '');
  }

  const versionsUrl = unversionedDocBaseUri + 'gds-doc-versions.json';
  const availableDocVersions = await jQuery.ajax({
    type: 'GET',
    url: versionsUrl,
    headers: {
      'Content-Type': 'application/json',
      Accept: 'application/vnd.github.v3+json'
    },
    crossOrigin: true,
    success: function (data) {
      return data;
    },
    error: function (jqXHR, textStatus, errorThrown) {
      console.error('Request failed...', jqXHR, textStatus, errorThrown);
    }
  });

  return {
    name: name,
    version: version,
    availableDocVersions: availableDocVersions,
    thisPubBaseUri: thisPubBaseUri,
    unversionedDocBaseUri: unversionedDocBaseUri,
    commonDocsBaseUri: commonDocsBaseUri,
    neo4jPageId: neo4jPageId
  }
})();
// vim: set sw=2 ts=2:
