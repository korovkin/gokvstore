package gokvstore_test

import (
	"testing"

	"github.com/korovkin/gokvstore"

	. "github.com/onsi/gomega"
)

func TestPQ(t *testing.T) {
	g := NewGomegaWithT(t)

	s, err := gokvstore.NewStorePostgresWithValueType(
		"test",
		"jsonb",
		"host=localhost user=test password=test dbname=test sslmode=disable",
		nil)

	g.Expect(err).To(BeNil())
	defer s.Close()

	s.DeleteAll()
	// defer s.DeleteAll()

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
	err = s.IterateByKeyPrefixASCEQ(
		"k",
		1000,
		func(k *string, t *string, v *string, stop *bool) {
			list = append(list, *k)
			list = append(list, *v)
		})
	g.Expect(err).To(BeNil())
	g.Expect(list).To(BeEquivalentTo([]string{"k", "3", "kk", "33", "kkk", "333"}))

	list = []string{}
	err = s.IterateByKeyPrefixDESCEQ(
		"z",
		1000,
		func(k *string, t *string, v *string, stop *bool) {
			list = append(list, *k)
			list = append(list, *v)
		})
	g.Expect(err).To(BeNil())
	g.Expect(list).To(BeEquivalentTo([]string{"kkk", "333", "kk", "33", "k", "3"}))

	list = []string{}
	err = s.IterateByKeyPrefixDESCEQ(
		"z",
		1000,
		func(k *string, t *string, v *string, stop *bool) {
			list = append(list, *k)
			list = append(list, *v)

			*stop = true
		})
	g.Expect(err).To(BeNil())
	g.Expect(list).To(BeEquivalentTo([]string{"kkk", "333"}))
}
