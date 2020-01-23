package gokvstore_test

import (
	"log"
	"os"
	"testing"

	"github.com/korovkin/gokvstore"

	. "github.com/onsi/gomega"
)

func TestFarmHasCow(t *testing.T) {
	g := NewGomegaWithT(t)

	deleteFile := func(filename string) {
		log.Println("=> removing:", filename)

		os.RemoveAll(filename)
	}
	defer func() {
		deleteFile("kv_test.db")
	}()

	deleteFile("kv_test.db")

	s, err := gokvstore.NewStoreSqlite("kv_test", ".")
	g.Expect(err).To(BeNil())
	defer s.Close()

	err = s.AddValueKVT("k", "1", "t")
	g.Expect(err).To(BeNil())
	s.AddValueKVT("k", "2", "t")
	g.Expect(err).To(BeNil())
	s.AddValueKVT("k", "3", "t")
	g.Expect(err).To(BeNil())

	s.AddValueKVT("kk", "33", "t")
	s.AddValueKVT("kkk", "333", "t")

	v := s.GetValue("k")
	g.Expect(v).NotTo(BeNil())
	g.Expect(*v).To(Equal("3"))

	v = s.GetValue("kk")
	g.Expect(v).NotTo(BeNil())
	g.Expect(*v).To(Equal("33"))

	v = s.GetValue("kkk")
	g.Expect(v).NotTo(BeNil())
	g.Expect(*v).To(Equal("333"))

	list := []string{}
	err = s.IterateByKeyPrefixASC(
		"k",
		1000,
		func(k *string, t *string, v *string, stop *bool) {
			list = append(list, *k)
			list = append(list, *v)
		})
	g.Expect(err).To(BeNil())
	g.Expect(list).To(BeEquivalentTo([]string{"k", "3", "kk", "33", "kkk", "333"}))

	list = []string{}
	err = s.IterateByKeyPrefixDESC(
		"zzz",
		1000,
		func(k *string, t *string, v *string, stop *bool) {
			list = append(list, *k)
			list = append(list, *v)
		})
	g.Expect(err).To(BeNil())
	g.Expect(list).To(BeEquivalentTo([]string{"kkk", "333", "kk", "33", "k", "3"}))
}
