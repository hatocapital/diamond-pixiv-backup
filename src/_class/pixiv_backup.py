import json
import random
import re
import time
from pathlib import Path

import boto3
from pixivpy3 import AppPixivAPI

class PixivBackup(AppPixivAPI):
    def __init__(self):
        super().__init__()
        with open("config.json", "r") as f:
            self.config = json.load(f)
        super().auth(refresh_token=self.config["refresh_token"])

    def bookmark_backup(self, restrict="private"):
        bookmark = super().user_bookmarks_illust(user_id=self.config["user_id"], restrict=restrict)
        self.__bookmark_download(bookmark)
        s3 = boto3.resource("s3", aws_access_key_id=self.config["aws_access_key_id"], aws_secret_access_key=self.config["aws_secret_access_key"])
        bucket = s3.Bucket(self.config["bucket"])
        with Path(self.config['export_dir']) as p:
            for f in p.glob("*/*"):
                bucket.upload_file(str(f), f"pixiv/backup/{f.parent.stem}/{f.name}", ExtraArgs={"ContentType": f"image/{f.suffix.lstrip('.')}", "StorageClass": "STANDARD_IA", "ServerSideEncryption": "AES256"})

    def __bookmark_download(self, bookmark: dict):
        count = 0
        for illust in bookmark.illusts:
            count += 1
            if count >= 30:
                next_url = bookmark.next_url
                next_qs = super().parse_qs(next_url)
                bookmark_n = super().user_bookmarks_illust(**next_qs)
                self.__bookmark_download(bookmark_n)
            else:
                with Path(f"{self.config['export_dir']}/{illust.user['name']}") as p:
                    p.mkdir(parents=True, exist_ok=True)
                    f_illust = list(filter(None, [re.search(f"^{illust.id}_.*(.png|.jpg)$", str(x.name)) for x in p.iterdir()]))
                    if not f_illust:
                        if illust.page_count >= 2: [super().download(url.image_urls.original, path=p) for url in illust.meta_pages]
                        else: super().download(illust.meta_single_page.original_image_url, path=p)
                        time.sleep(random.randint(2, 4))
                    else:
                        continue
