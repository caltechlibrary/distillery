% rebase("base.tpl")
% if error:
<p>{{error}}
% end
<form action="/" method="post">
    <label for="collection_id">CollectionID</label>
    <input id="collection_id" name="collection_id" type="text">
    <button>🚀</button>
</form>
