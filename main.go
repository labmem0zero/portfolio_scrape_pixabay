package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	"github.com/chromedp/cdproto/runtime"
	"github.com/chromedp/chromedp"
	_ "github.com/mattn/go-sqlite3"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type ScrapedImage struct{
	Url string
	Tags string
}

type Picture struct{
	picid string
	picdir string
	tags string
	url string
	used int
	searchtag string
}

var (
	cookieDir string
	db *sql.DB
	ctx context.Context
	schemaSQL = `
		CREATE TABLE IF NOT EXISTS pictures (
			picid TEXT UNIQUE,
			filedir TEXT,
			originalurl TEXT UNIQUE,
			used INTEGER,
			searchtag TEXT
		);

		CREATE TABLE IF NOT EXISTS tags (
			picid TEXT,
			tag TEXT
		);

		CREATE INDEX IF NOT EXISTS picid ON pictures(picid);
		`
	insertPictures = `
		INSERT INTO pictures (
			picid, filedir, originalurl, used, searchtag
		) VALUES (
			?, ?, ?, ?, ?
		)
		`
	insertTags = `
	INSERT INTO tags (
		picid, tag
	) VALUES (
		?, ?
	)
	`
)

func AddPictureToDb(pic Picture) error{
	_, err:=db.Exec(insertPictures, pic.picid, pic.picdir, pic.url, pic.used, pic.searchtag)
	if err!=nil{
		return err
	}
	AddTagsToDb(pic.picid, pic.tags)
	fmt.Println("В БД добавлена следующая запись: ", pic)
	return nil
}

func AddTagsToDb(picid string, pictags string)  {
	for _, tag:=range strings.Split(pictags,",") {
		_, err:=db.Exec(insertTags, picid, tag)
		if err!=nil{
			fmt.Println("При добавлении тэга к изображению с ID='",picid,"' взникла ошибка: ",err)
			continue
		}
		fmt.Println("В таблиц tags добавлен тэг'",tag,"' к изображению '",picid,"'")
	}
}

func ScrapeImages(html string) ([]ScrapedImage,error){
	var images []ScrapedImage
	var image ScrapedImage
	var tempHtml string
	tempHtml=html
	tempReader:=strings.NewReader(html)
	fmt.Println("Скрапим страницу")
	doc, err:=goquery.NewDocumentFromReader(tempReader)
	if err!=nil{
		return images, err
	}
	//Нахожу ссылки на страницы всех изображений со страницы, которые являются результатами поиска
	doc.Find(`div[class*='result'] div[class*='container'] > a`).Each(func(_ int, img *goquery.Selection) {
		fmt.Println("Скрап")
		image.Url=""
		image.Tags=""
		tmpUrl,_:=img.Attr("href")
		tmpUrl=tmpUrl
		//перехожу на страницу каждого изображения, что бы получить ссылку на изображение в большом размере
		err = chromedp.Run(ctx,
			chromedp.Navigate(tmpUrl),
			chromedp.Sleep(500*time.Millisecond),
			chromedp.Click(".download_menu.large_menu", chromedp.ByQuery),
			chromedp.Sleep(500*time.Millisecond),
			chromedp.InnerHTML("*", &tempHtml, chromedp.ByQuery),
		)
		imgDoc, err := goquery.NewDocumentFromReader(strings.NewReader(tempHtml))
		if err != nil {
			fmt.Println(err)
		}

		//нельзя забывать, что для внутренних переходов авторы сайтов могут
		//опускать доменное имя в href. Так, например, сделано на этом сайте(pixabay.com)
		//поэтому я добавляю вычлененный адрес картинки к доменному имени,
		//что бы получить полный адрес картинки
		image.Url, _ = imgDoc.Find(`a[href*='attachment']`).First().Attr("href")
		image.Url="https://pixabay.com"+strings.Split(image.Url,"?")[0]
		fmt.Println("Добавлено в список загрузок: ",image.Url)

		var tags = make([]string, 0)
		imgDoc.Find(`.tags > a`).Each(func(_ int, elem *goquery.Selection) {
			if strings.Contains(elem.Text()," ")==false && strings.Contains(elem.Text(),"-")==false{
				tags = append(tags, elem.Text())
			}
		})
		image.Tags=strings.Join(tags,",")
		images=append(images,image)
	})
	return images, nil
}

func UrlExistance(url string)bool{
	var res string
	//Перехожу на указанный URL, что бы проверить страницу на существование
	//Если url респонса не равен введенному url то страницы не существует
	//Стандартный http.get всегда будет давать error 402, поэтому через хромдп делаю
	//Нужно обратить внимание, что необходимо всегда заранее делать queryescape, во избежании случаев
	//когда браузер автоматически заменяет кириллицу кодами, иначе будет ложное срабатывание функции
	resp, err := chromedp.RunResponse(ctx,
		chromedp.Navigate(url),
		chromedp.ActionFunc(func(ctx context.Context) error {
			fmt.Println("Проверяем URL: ",url)
			time.Sleep(100*time.Millisecond)
			return nil
		}),
	)
	if err != nil {
		fmt.Println(err)
	}
	res=resp.URL

	if res==url{
		return true
	}else{
		return false
	}
}

func DownloadScrapped(imglist []ScrapedImage, key string, page int){
	tmpDbPic:=Picture{}
	for imgnumber, img:=range imglist{
		tmpNumber:="0"
		if imgnumber<10{
			tmpNumber="00"+strconv.Itoa(imgnumber)
		}else{
			if imgnumber<100 {
				tmpNumber="0"+strconv.Itoa(imgnumber)
			}else {
				tmpNumber=strconv.Itoa(imgnumber)
			}
		}
		extension:=img.Url[len(img.Url)-4:len(img.Url)]
		tmpdir:="C:/scrapper2021/images/"+key+"/"+strconv.Itoa(page)+"/"+tmpNumber+extension
		tmp:=img.Url
		tmp=strings.ReplaceAll(tmp,"//","/")
		tmp=strings.Split(tmp,"/")[1]
		tmp=strings.Join(strings.Split(tmp,".")[len(strings.Split(tmp,"."))-2:],".")
		tmpDbPic.picid=tmp+"_"+key+"_"+strconv.Itoa(page)+"_"+tmpNumber
		tmpDbPic.picdir=tmpdir
		//во избежание очевидных проблем удаляю задвоенные запятые
		tmpDbPic.tags=strings.ReplaceAll(img.Tags,",,",",")
		tmpDbPic.searchtag=key
		tmpDbPic.used=0
		fmt.Printf("Загружаем файл '%v'\n",img.Url)
		res,err:=chromedp.RunResponse(ctx,
			chromedp.Navigate(img.Url),
			chromedp.ActionFunc(func(ctx context.Context) error {
				time.Sleep(100*time.Millisecond)
				return nil
			}),
		)
		img.Url=res.URL
		tmpDbPic.url=img.Url
		err=AddPictureToDb(tmpDbPic)
		resp,err:=http.Get(img.Url)
		if err!=nil{
			fmt.Println("При добавлении записи в бд произошла ошибка: ",err)
			continue
		}
		defer resp.Body.Close()
		imgfile,err:=os.Create(tmpdir)
		defer imgfile.Close()
		if err != nil {
			fmt.Println("Ошибка при создании пустого файла: ",err)
			continue
		}
		_, err= io.Copy(imgfile, resp.Body)
		if err != nil {
			fmt.Println("Ошибка при копировании изображения в файл: ",err)
			continue
		}
		fmt.Println("Файл успешно сохранен: ",tmpDbPic.picid)
		time.Sleep(200*time.Millisecond)
	}
}

func GetHttpHtmlContent(url string) (string, error) {
	var htmlContent string
	err := chromedp.Run(ctx,
		chromedp.Navigate(url),
		chromedp.ActionFunc(func(ctx context.Context) error {
			fmt.Println("Скроллим")
			time.Sleep(100*time.Millisecond)
			_, exp, err := runtime.Evaluate(`window.scrollTo(0,document.body.scrollHeight);`).Do(ctx)
			if err != nil {
				return err
			}
			if exp != nil {
				return exp
			}

			time.Sleep(100*time.Millisecond)
			_, exp, err = runtime.Evaluate(`window.scrollTo(0,0);`).Do(ctx)
			if err != nil {
				return err
			}
			if exp != nil {
				return exp
			}

			time.Sleep(100*time.Millisecond)
			_, exp, err = runtime.Evaluate(`window.scrollTo(0,document.body.scrollHeight);`).Do(ctx)
			if err != nil {
				return err
			}
			if exp != nil {
				return exp
			}

			return nil
		}),
		chromedp.InnerHTML("*", &htmlContent, chromedp.ByQuery),
	)
	if err != nil {
		fmt.Println("Ошибка при обработке запросов к браузеру :", err)
		return "", err
	}

	return htmlContent, nil
}

func KeywordScrapping(tempkey string, urlmask string, start int, end int) {
	//возвращаю кириллицу
	key,err:=url.QueryUnescape(tempkey)
	if err!=nil{
		fmt.Println("Ошибка с запросом ключевого слова, завершаем")
		return
	}

	looper:=start
	imglist:=[]ScrapedImage{}
	url:=""
	//цикл работает, пока по маске урла выдаются существующие страницы
	for looper:=start;looper<end+1;looper++{
		url=strings.ReplaceAll(strings.ReplaceAll(urlmask,"keyword",tempkey),"page",strconv.Itoa(looper))
		if UrlExistance(url)==false{
			fmt.Printf("Ошибка, страницы %v с ключевым словом %v не существует, завершаем цикл\n",looper,key)
			break
		}
		//каждую итерацию очищаю срез
		imglist=nil
		imglist=[]ScrapedImage{}
		fmt.Println("URL=",url)
		err:=os.MkdirAll("C:/scrapper2021/images/"+key+"/"+strconv.Itoa(looper), 0777)
		fmt.Println("Создана папка: C:/scrapper2021/images/"+key+"/"+strconv.Itoa(looper))
		res, _:=GetHttpHtmlContent(url)
		if res!=""{
			fmt.Println("скачан хтмл контент")
		}else{
			fmt.Printf("Не удалось загрузить конент страницы: %v",url)
			continue
		}
		imglist, err=ScrapeImages(res)
		if err!=nil{
			fmt.Printf("Ошибка при скрапинге изображений по URL %v: %v",url,err)
			continue
		}
		DownloadScrapped(imglist, key, looper)
		time.Sleep(1*time.Second)
		if looper==end{
			fmt.Println("Заскраплена последняя страница")
			break
		}
	}
	fmt.Println("Заскраплены страницы с ",start," до ",looper)
}

func FirstRun(){
	err:=chromedp.Run(ctx,
		chromedp.Navigate("https://pixabay.com"),
		chromedp.Sleep(60*time.Minute),
		)
	if err!=nil{
		fmt.Printf("При запуске хромдп произошла ошибка: %v\n",err)
	}
}

func main() {
	cookieDir = "cookie"
	if _, err := os.Stat(cookieDir); os.IsNotExist(err) {
		err := os.Mkdir(cookieDir, os.ModePerm)
		if err != nil {
			fmt.Println("Ошибка при срздании папки с куки: ",err)
		}
	}
	cookieDir, _ = filepath.Abs(cookieDir)
	//открываю бд
	sqldb, err := sql.Open("sqlite3", "C:\\scrapper2021\\pictures.db")
	db=sqldb
	if err != nil {
		fmt.Println("Ошибка при создании/открытии бд: ",err)
	}
	/*_, err=sqlDB.Exec(`DROP TABLE pictures;`)
	if err != nil {
		fmt.Println("Ошибка при удалении таблицы 'pictures': ",err)
	}*/
	_, err= db.Exec(schemaSQL)
	if err != nil {
		fmt.Println("Ошибка при создании таблицы 'pictures': ",err)
	}
	defer db.Close()

	//объявляю опции для хромдп контекстов. В идеале использовать headless=true,
	//но по недокумментированным причинам, в таком случае не все изображения прогружаются
	options := []chromedp.ExecAllocatorOption{
		chromedp.NoDefaultBrowserCheck,
		chromedp.Flag("headless", false),
		chromedp.Flag("no-first-run", true),
		chromedp.Flag("no-sandbox", true),
		chromedp.Flag("ignore-certificate-errors", true),
		chromedp.Flag("user-data-dir", cookieDir),
	}
	options = append(chromedp.DefaultExecAllocatorOptions[:], options...)
	c, _ := chromedp.NewExecAllocator(context.Background(), options...)
	chromeCtx, cancel := chromedp.NewContext(c, chromedp.WithLogf(log.Printf))
	chromedp.Run(chromeCtx, make([]chromedp.Action, 0, 1)...)
	additionalCtx, cancel2 := context.WithCancel(chromeCtx)
	defer cancel2()
	defer cancel()
	ctx=additionalCtx

	// ловлю сигналы завершения и закрываю браузер
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		_ = <-sigChan
		cancel2()
		cancel()
	}()

	urlmask:="https://pixabay.com/ru/images/search/keyword/?pagi=page"
	//задаю параметры для скраппинга: кейворд, нач. страница, конеч. страница
	//Не обязательно, но рекомендуется: заранее посмотреть, сколько страниц существует по запросу
	//т.к. некоторые запросы в результате дают 1-2 страницы
	//можно реализовать скрап с первой страницы и до конца, скачивая все изображения по запросу
	//но решил добавить пользователю возможность скрапить по частям,
	//ибо это элементарно позволит не искать место под изображения
	//для запросов с большим количеством результатов
	//да и вообще, дискретность - наше всё
	keyword:="пёсик"
	start:=1
	end:=1
	//превращаю кириллицу в удобочитаемый для браузера текст, необходимо для проверки существования страниц
	keyword=url.QueryEscape(keyword)
	//Для первого запуска нужно раскомментировать 'FirstRun()' в коде ниже
	//запустить программу и залогиниться на открывшемся сайте
	//После этого нужно остановить работу программы, закомментировать функцию обратно и запустить снова
	//Благодаря этому будут прописаны куки с логином и паролем
	//Это - необходимый "костыль", без которого не получится скачивать изображения в высоком качестве
	//Автоматизировать данный процесс не представляется простой задачей,
	//поскольку  сайт при логине предлагает пройти капчу
	//FirstRun()
	KeywordScrapping(keyword, urlmask, start, end)
	if err!=nil{
		fmt.Println("Ощибка при скраппинге: ",err)
	}
}